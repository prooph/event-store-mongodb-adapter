<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 08/06/15 - 21:58
 */

namespace Prooph\EventStore\Adapter\MongoDb;

use Assert\Assertion;
use DateTimeInterface;
use Iterator;
use MongoDB\BSON\UTCDatetime;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;
use MongoDB\Operation\CreateIndexes;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFoundException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Rhumsaa\Uuid\Uuid;

/**
 * EventStore Adapter for Mongo DB
 *
 * Class MongoDBEventStoreAdapter
 * @package Prooph\EventStore\Adapter\MongoDb
 */
final class MongoDBEventStoreAdapter implements Adapter, CanHandleTransaction
{
    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    /**
     * @var Manager
     */
    private $manager;

    /**
     * @var string
     */
    private $dbName;

    /**
     * @var StreamName
     */
    private $currentStreamName;

    /**
     * The transaction id, if currently in transaction, otherwise null
     *
     * @var Uuid|null
     */
    private $transactionId;

    /**
     * @var WriteConcern|null
     */
    private $writeConcern;

    /**
     * Transaction timeout in seconds
     *
     * @var int
     */
    private $transactionTimeout = 50;

    /**
     * Custom sourceType to collection mapping
     *
     * @var array
     */
    private $streamCollectionMap = [];

    /**
     * @var ReadPreference
     */
    private $readPreference;

    /**
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param Manager $manager
     * @param string $dbName
     * @param WriteConcern|null $writeConcern
     * @param int|null $transactionTimeout
     * @param array $streamCollectionMap
     */
    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        Manager $manager,
        $dbName,
        WriteConcern $writeConcern = null,
        $transactionTimeout = null,
        array $streamCollectionMap = []
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->messageFactory   = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->manager          = $manager;
        $this->dbName           = $dbName;
        $this->streamCollectionMap = $streamCollectionMap;
        $this->readPreference   = new ReadPreference(ReadPreference::RP_PRIMARY);

        if (null === $writeConcern) {
            $writeConcern = new WriteConcern(1, 0, true);
        }

        $this->writeConcern = $writeConcern;

        if (null !== $transactionTimeout) {
            Assertion::min($transactionTimeout, 1, 'Transaction timeout must be a positive integer');

            $this->transactionTimeout = $transactionTimeout;
        }
    }

    /**
     * @param Stream $stream
     * @throws RuntimeException If creation of stream fails
     * @return void
     */
    public function create(Stream $stream)
    {
        if (!$stream->streamEvents()->valid()) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $streamName = $stream->streamName();

        $this->createIndexes($streamName);

        $this->appendTo($streamName, $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param Iterator $streamEvents
     * @throws StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents)
    {
        if ($this->currentStreamName !== null && $this->currentStreamName->toString() !== $streamName->toString()) {
            throw new \RuntimeException('Cannot write to different stream streams in one transaction');
        }

        $this->currentStreamName = $streamName;

        $bulk = new BulkWrite(['ordered' => false]);

        foreach ($streamEvents as $streamEvent) {
            $data = $this->prepareEventData($streamName, $streamEvent);
            $bulk->insert($data);
        }

        //var_dump($data);
        //try {

            $this->manager->executeBulkWrite(
                $this->dbName . '.' . $this->getCollectionName($this->currentStreamName),
                $bulk,
                $this->writeConcern
            );
//        } catch (\Exception $e) {
//            var_dump(get_class($e));
//            var_dump($e->getMessage());
//            die;
//        }
    }

    /**
     * @param StreamName $streamName
     * @param Message $e
     * @return array
     */
    private function prepareEventData(StreamName $streamName, Message $e)
    {
        $eventArr = $this->messageConverter->convertToArray($e);

        $eventData = [
            '_id'         => $eventArr['uuid'],
            'version'     => $eventArr['version'],
            'event_name'  => $eventArr['message_name'],
            'payload'     => $eventArr['payload'],
            'created_at'  => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        foreach ($eventArr['metadata'] as $key => $value) {
            $eventData[$key] = (string) $value;
        }

        if (null !== $this->transactionId) {
            $eventData['transaction_id'] = $this->transactionId->toString();

            $time = time() . '000';

            $eventData['expire_at'] = new UTCDatetime($time);
        }

        return $eventData;
    }

    /**
     * @param StreamName $streamName
     * @param null|int $minVersion
     * @return Stream|null
     */
    public function load(StreamName $streamName, $minVersion = null)
    {
        $events = $this->loadEvents($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return MongoDBStreamIterator
     * @throws StreamNotFoundException
     */
    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        $query = $metadata;

        if (null !== $minVersion) {
            $query['version'] = ['$gte' => $minVersion];
        }

        $query['expire_at'] = ['$exists' => false];
        $query['transaction_id'] = ['$exists' => false];

        $query = new Query($query, [
            'sort' => [
                'version' => 1
            ]
        ]);

        $namespace = $this->dbName . '.' . $this->getCollectionName($streamName);

        return new MongoDBStreamIterator($this->manager, $namespace, $query, $this->readPreference, $this->messageFactory, $metadata);
    }

    /**
     * @param StreamName $streamName
     * @param DateTimeInterface|null $since
     * @param array $metadata
     * @return MongoDBStreamIterator
     */
    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        $query = $metadata;

        if (null !== $since) {
            $query['created_at'] = ['$gt' => $since->format('Y-m-d\TH:i:s.u')];
        }

        $query['expire_at'] = ['$exists' => false];
        $query['transaction_id'] = ['$exists' => false];

        $query = new Query($query, [
            'sort' => [
                'created_at' => 1,
                'version' => 1
            ]
        ]);

        $namespace = $this->dbName . '.' . $this->getCollectionName($streamName);

        return new MongoDBStreamIterator($this->manager, $namespace, $query, $this->readPreference, $this->messageFactory, $metadata);
    }

    /**
     * Begin transaction
     *
     * @return void
     */
    public function beginTransaction()
    {
        if (null !== $this->transactionId) {
            throw new \RuntimeException('Transaction already started');
        }

        $this->transactionId = Uuid::uuid4();
    }

    /**
     * Commit transaction
     *
     * @return void
     */
    public function commit()
    {
        if (! $this->currentStreamName) {
            $this->transactionId = null;
            return;
        }

        $bulk = new BulkWrite(['ordered' => false]);
        $bulk->update(
            [
                'transaction_id' => $this->transactionId->toString(),
                '$isolated' => 1,
            ],
            [
                '$unset' => [
                    'expire_at' => 1,
                    'transaction_id' => 1
                ]
            ],
            [
                'multi' => true
            ]
        );

        $this->manager->executeBulkWrite(
            $this->dbName . '.' . $this->getCollectionName($this->currentStreamName),
            $bulk,
            $this->writeConcern
        );

        $this->transactionId = null;
        $this->currentStreamName = null;
    }

    /**
     * Rollback transaction
     *
     * @return void
     */
    public function rollback()
    {
        if (! $this->currentStreamName) {
            $this->transactionId = null;
            return;
        }

        $bulk = new BulkWrite(['ordered' => false]);
        $bulk->delete(
            [
                'transaction_id' => $this->transactionId
            ],
            [
                'limit' => 0
            ]
        );

        $this->manager->executeBulkWrite(
            $this->dbName . '.' . $this->getCollectionName($this->currentStreamName),
            $bulk,
            $this->writeConcern
        );
    }

    /**
     * @param StreamName $streamName
     * @return void
     */
    private function createIndexes(StreamName $streamName)
    {
        $createIndexes = new CreateIndexes($this->dbName, $this->getCollectionName($streamName), [
            [
                'key' => [
                    '_id' => 1,
                    'transaction_id' => 1,
                ],
                'unique' => true,
                'background' => true,
            ],
            [
                'key' => [
                    'expire_at' => 1,
                ],
                'expireAfterSeconds' => $this->transactionTimeout,
            ]
        ]);

        $createIndexes->execute($this->manager->selectServer($this->readPreference));
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getCollectionName(StreamName $streamName)
    {
        if (isset($this->streamCollectionMap[$streamName->toString()])) {
            $collectionName = $this->streamCollectionMap[$streamName->toString()];
        } else {
            $collectionName = strtolower($this->getShortStreamName($streamName));
            if (strpos($collectionName, "_stream") === false) {
                $collectionName.= "_stream";
            }
        }

        return $collectionName;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }
}
