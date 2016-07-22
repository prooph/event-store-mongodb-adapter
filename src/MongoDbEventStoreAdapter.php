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
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFoundException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * EventStore Adapter for Mongo DB
 *
 * Class MongoDbEventStoreAdapter
 * @package Prooph\EventStore\Adapter\MongoDb
 */
final class MongoDbEventStoreAdapter implements Adapter, CanHandleTransaction
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
     * @var \MongoClient
     */
    private $mongoClient;

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
     * @var \MongoId|null
     */
    private $transactionId;

    /**
     * Mongo DB write concern
     * The default options can be overridden with the constructor
     *
     * @var array
     */
    private $writeConcern = [
        'w' => 1,
    ];

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
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param \MongoClient $mongoClient
     * @param string $dbName
     * @param array|null $writeConcern
     * @param int|null $transactionTimeout
     * @param array $streamCollectionMap
     */
    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        \MongoClient $mongoClient,
        $dbName,
        array $writeConcern = null,
        $transactionTimeout = null,
        array $streamCollectionMap = []
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->messageFactory   = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->mongoClient      = $mongoClient;
        $this->dbName           = $dbName;
        $this->streamCollectionMap = $streamCollectionMap;

        if (null !== $writeConcern) {
            $this->writeConcern = $writeConcern;
        }

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
            throw new \RuntimeException('Cannot write to different streams in one transaction');
        }

        $this->currentStreamName = $streamName;

        $insertBatch = $this->getInsertBatch($streamName);

        foreach ($streamEvents as $streamEvent) {
            $data = $this->prepareEventData($streamName, $streamEvent);
            $insertBatch->add($data);
        }

        try {
            $insertBatch->execute();
        } catch (\MongoWriteConcernException $e) {
            $code = $e->getDocument()['writeErrors'][0]['code'];
            if (in_array($code, [11000, 11001, 12582])) {
                throw new ConcurrencyException('At least one event with same version exists already', 0, $e);
            }
        }
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
            $eventData['transaction_id'] = $this->transactionId;
            $eventData['expire_at'] = new \MongoDate();
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
     * @return MongoDbStreamIterator
     * @throws StreamNotFoundException
     */
    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        $collection = $this->getCollection($streamName);

        $query = $metadata;

        if (null !== $minVersion) {
            $query['version'] = ['$gte' => $minVersion];
        }

        $query['expire_at'] = ['$exists' => false];
        $query['transaction_id'] = ['$exists' => false];

        $cursor = $collection->find($query)->sort(['version' => $collection::ASCENDING]);

        return new MongoDbStreamIterator($cursor, $this->messageFactory, $metadata);
    }

    /**
     * @param StreamName $streamName
     * @param DateTimeInterface|null $since
     * @param array $metadata
     * @return MongoDbStreamIterator
     */
    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        $collection = $this->getCollection($streamName);

        $query = $metadata;

        if (null !== $since) {
            $query['created_at'] = ['$gt' => $since->format('Y-m-d\TH:i:s.u')];
        }

        $query['expire_at'] = ['$exists' => false];
        $query['transaction_id'] = ['$exists' => false];

        $cursor = $collection->find($query)->sort([
            'created_at' => $collection::ASCENDING,
            'version' => $collection::ASCENDING
        ]);

        return new MongoDbStreamIterator($cursor, $this->messageFactory, $metadata);
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

        $this->transactionId = new \MongoId();
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

        $updateBatch = $this->getUpdateBatch($this->currentStreamName);

        $updateBatch->add(
            [
                'q' => [
                    'transaction_id' => $this->transactionId,
                    '$isolated' => 1
                ],
                'u' => [
                    '$unset' => [
                        'expire_at' => 1,
                        'transaction_id' => 1
                    ]
                ],
                'multi' => true
            ]
        );

        $updateBatch->execute();

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

        $deleteBatch = $this->getDeleteBatch($this->currentStreamName);

        $deleteBatch->add([
            'q' => [
                'transaction_id' => $this->transactionId
            ],
            'limit' => 0
        ]);

        $deleteBatch->execute();

        $this->transactionId = null;
        $this->currentStreamName = null;
    }

    /**
     * @param StreamName $streamName
     * @return void
     */
    private function createIndexes(StreamName $streamName)
    {
        $collection = $this->getCollection($streamName);

        $collection->createIndex(
            [
                'aggregate_id' => 1,
                'version' => 1,
            ],
            [
                'unique' => true,
            ]
        );

        $collection->createIndex(
            [
                'created_at' => 1,
                'version' => 1,
            ]
        );
    }

    /**
     * Get mongo db stream collection
     *
     * @param StreamName $streamName
     * @return \MongoCollection
     */
    private function getCollection(StreamName $streamName)
    {
        $collection = $this->mongoClient->selectCollection($this->dbName, $this->getCollectionName($streamName));
        $collection->setReadPreference(\MongoClient::RP_PRIMARY);

        return $collection;
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

    /**
     * Get mongo db insert batch
     *
     * @param StreamName $streamName
     * @return \MongoInsertBatch
     */
    private function getInsertBatch(StreamName $streamName)
    {
        return new \MongoInsertBatch($this->getCollection($streamName), $this->writeConcern);
    }

    /**
     * Get mongo db update batch
     *
     * @param StreamName $streamName
     * @return \MongoUpdateBatch
     */
    private function getUpdateBatch(StreamName $streamName)
    {
        return new \MongoUpdateBatch($this->getCollection($streamName), $this->writeConcern);
    }

    /**
     * Get mongo db delete batch
     *
     * @param StreamName $streamName
     * @return \MongoDeleteBatch
     */
    private function getDeleteBatch(StreamName $streamName)
    {
        return new \MongoDeleteBatch($this->getCollection($streamName), $this->writeConcern);
    }
}
