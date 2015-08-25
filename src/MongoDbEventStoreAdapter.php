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
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
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
     * @var \MongoCollection
     */
    private $collection;

    /**
     * @var \MongoDeleteBatch
     */
    private $deleteBatch;

    /**
     * @var \MongoInsertBatch
     */
    private $insertBatch;

    /**
     * @var \MongoUpdateBatch
     */
    private $updateBatch;

    /**
     * @var string
     */
    private $dbName;

    /**
     * Name of the mongo db collection
     *
     * @var string
     */
    private $streamCollectionName = 'event_stream';

    /**
     * @var array
     */
    private $standardColumns = [
        '_id',
        'event_name',
        'created_at',
        'payload',
        'version',
        'transaction_id',
        'expire_at'
    ];

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
        'j' => true
    ];

    /**
     * Transaction timeout in seconds
     *
     * @var int
     */
    private $transactionTimeout = 50;

    /**
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param \MongoClient $mongoClient
     * @param string $dbName
     * @param array|null $writeConcern
     * @param string|null $streamCollectionName
     * @param int|null $transactionTimeout
     */
    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        \MongoClient $mongoClient,
        $dbName,
        array $writeConcern = null,
        $streamCollectionName = null,
        $transactionTimeout = null
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->messageFactory   = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->mongoClient      = $mongoClient;
        $this->dbName           = $dbName;

        if (null !== $streamCollectionName) {
            Assertion::minLength($streamCollectionName, 1, 'Stream collection name must be a string with min length 1');

            $this->streamCollectionName = $streamCollectionName;
        }

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
        if (count($stream->streamEvents()) === 0) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $this->createIndexes();

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param Message[] $streamEvents
     * @throws StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $streamName, array $streamEvents)
    {
        foreach ($streamEvents as $streamEvent) {
            $data = $this->prepareEventData($streamName, $streamEvent);
            $this->getInsertBatch()->add($data);
        }

        $this->getInsertBatch()->execute();
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
        $events = $this->loadEventsByMetadataFrom($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return Message[]
     * @throws StreamNotFoundException
     */
    public function loadEventsByMetadataFrom(StreamName $streamName, array $metadata, $minVersion = null)
    {
        $collection = $this->getCollection();

        $query = $metadata;

        if (null !== $minVersion) {
            $query['version'] = ['$gte' => $minVersion];
        }

        $query['expire_at'] = ['$exists' => false];
        $query['transaction_id'] = ['$exists' => false];

        $results = $collection->find($query)->sort(['version' => $collection::ASCENDING]);

        $events = [];

        foreach ($results as $eventData) {
            //Add metadata stored in table
            foreach ($eventData as $key => $value) {
                if (! in_array($key, $this->standardColumns)) {
                    $metadata[$key] = $value;
                }
            }

            $createdAt = \DateTimeImmutable::createFromFormat(
                'Y-m-d\TH:i:s.u',
                $eventData['created_at'],
                new \DateTimeZone('UTC')
            );

            $events[] = $this->messageFactory->createMessageFromArray($eventData['event_name'], [
                'uuid' => $eventData['_id'],
                'version' => (int) $eventData['version'],
                'created_at' => $createdAt,
                'payload' => $eventData['payload'],
                'metadata' => $metadata
            ]);
        }

        if (empty($events)) {
            throw new StreamNotFoundException(
                sprintf(
                    'Stream with name %s cannot be found',
                    $streamName->toString()
                )
            );
        }

        return $events;
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
        $this->getUpdateBatch()->add(
            [
                'q' => [
                    'transaction_id' => $this->transactionId
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

        $this->getUpdateBatch()->execute();

        $this->transactionId = null;
    }

    /**
     * Rollback transaction
     *
     * @return void
     */
    public function rollback()
    {
        $this->getDeleteBatch()->add([
            'q' => [
                'transaction_id' => $this->transactionId
            ],
            'limit' => 0
        ]);

        $this->getDeleteBatch()->execute();

        $this->transactionId = null;
    }

    /**
     * @return void
     */
    private function createIndexes()
    {
        $collection = $this->getCollection();

        $collection->createIndex(
            [
                '_id' => 1,
                'transaction_id' => 1
            ],
            [
                'unique' => true,
                'background' => true
            ]
        );

        $collection->createIndex(
            [
                'expire_at' => 1
            ],
            [
                'expireAfterSeconds' => $this->transactionTimeout,
            ]
        );
    }

    /**
     * Get mongo db stream collection
     *
     * @return \MongoCollection
     */
    private function getCollection()
    {
        if (null === $this->collection) {
            $this->collection = $this->mongoClient->selectCollection($this->dbName, $this->streamCollectionName);
            $this->collection->setReadPreference(\MongoClient::RP_PRIMARY);
        }

        return $this->collection;
    }

    /**
     * Get mongo db insert batch
     *
     * @return \MongoInsertBatch
     */
    private function getInsertBatch()
    {
        if (null === $this->insertBatch) {
            $this->insertBatch = new \MongoInsertBatch($this->getCollection(), $this->writeConcern);
        }

        return $this->insertBatch;
    }

    /**
     * Get mongo db update batch
     *
     * @return \MongoUpdateBatch
     */
    private function getUpdateBatch()
    {
        if (null === $this->updateBatch) {
            $this->updateBatch = new \MongoUpdateBatch($this->getCollection(), $this->writeConcern);
        }

        return $this->updateBatch;
    }

    /**
     * Get mongo db delete batch
     *
     * @return \MongoDeleteBatch
     */
    private function getDeleteBatch()
    {
        if (null === $this->deleteBatch) {
            $this->deleteBatch = new \MongoDeleteBatch($this->getCollection(), $this->writeConcern);
        }

        return $this->deleteBatch;
    }
}
