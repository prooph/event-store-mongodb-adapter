<?php

namespace Prooph\EventStore\Adapter\MongoDb;

use Assert\Assertion;
use Prooph\Common\Messaging\DomainEvent;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFoundException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * EventStore Adapter for Mongo DB
 */
class MongoDbEventStoreAdapter implements Adapter, CanHandleTransaction
{
    /**
     * @var \MongoClient
     */
    protected $mongoClient;

    /**
     * @var \MongoCollection
     */
    protected $collection;

    /**
     * @var \MongoDeleteBatch
     */
    protected $deleteBatch;

    /**
     * @var \MongoInsertBatch
     */
    protected $insertBatch;

    /**
     * @var \MongoUpdateBatch
     */
    protected $updateBatch;

    /**
     * @var string
     */
    protected $dbName;

    /**
     * Name of the mongo db collection
     *
     * @var string
     */
    protected $streamCollectionName = 'event_stream';

    /**
     * @var array
     */
    protected $standardColumns = [
        '_id',
        'created_at',
        'event_name',
        'event_class',
        'expire_at',
        'payload',
        'stream_name',
        'transaction_id',
        'version'
    ];

    /**
     * The transaction id, if currently in transaction, otherwise null
     *
     * @var \MongoId|null
     */
    protected $transactionId;

    /**
     * Mongo DB write concern
     * The default options can be overridden with the constructor
     *
     * @var array
     */
    protected $writeConcern = [
        'w' => 1,
        'j' => true
    ];

    /**
     * Transaction timeout in seconds
     *
     * @var int
     */
    protected $transactionTimeout = 50;

    /**
     * @param \MongoClient $mongoClient
     * @param string $dbName
     * @param array $writeConcern
     * @param string|null $streamCollectionName
     * @param int $transactionTimeout
     */
    public function __construct(
        \MongoClient $mongoClient,
        $dbName,
        array $writeConcern = null,
        $streamCollectionName = null,
        $transactionTimeout = null
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->mongoClient = $mongoClient;
        $this->dbName      = $dbName;

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
     * @param DomainEvent[] $streamEvents
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
     * @param DomainEvent $e
     * @return array
     */
    protected function prepareEventData(StreamName $streamName, DomainEvent $e)
    {
        $eventData = [
            '_id'         => $e->uuid()->toString(),
            'stream_name' => (string) $streamName,
            'version'     => $e->version(),
            'event_name'  => $e->messageName(),
            'event_class' => get_class($e),
            'payload'     => $e->payload(),
            'created_at'  => new \MongoDate($e->createdAt()->getTimestamp()),
        ];

        foreach ($e->metadata() as $key => $value) {
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
     * @return DomainEvent[]
     * @throws StreamNotFoundException
     */
    public function loadEventsByMetadataFrom(StreamName $streamName, array $metadata, $minVersion = null)
    {
        $collection = $this->getCollection();

        if (null !== $minVersion) {
            $metadata['version'] = ['$gte' => $minVersion];
        }

        $metadata['expire_at'] = ['$exists' => false];
        $metadata['transaction_id'] = ['$exists' => false];

        $results = $collection->find($metadata)->sort(['version' => $collection::ASCENDING]);

        $events = [];

        foreach ($results as $eventData) {
            $eventClass = $eventData['event_class'];

            //Add metadata stored in table
            foreach ($eventData as $key => $value) {
                if (! in_array($key, $this->standardColumns)) {
                    $metadata[$key] = $value;
                }
            }

            $createdAt = new \DateTime();
            $createdAt->setTimestamp($eventData['created_at']->sec);

            $events[] = $eventClass::fromArray(
                [
                    'uuid' => $eventData['_id'],
                    'name' => $eventData['event_name'],
                    'version' => (int) $eventData['version'],
                    'created_at' => $createdAt->format(\DateTime::ISO8601),
                    'payload' => $eventData['payload'],
                    'metadata' => $metadata
                ]
            );
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
            throw new \RuntimeException('Transaction already startet');
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
    protected function createIndexes()
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
    protected function getCollection()
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
    protected function getInsertBatch()
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
    protected function getUpdateBatch()
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
    protected function getDeleteBatch()
    {
        if (null === $this->deleteBatch) {
            $this->deleteBatch = new \MongoDeleteBatch($this->getCollection(), $this->writeConcern);
        }

        return $this->deleteBatch;
    }
}
