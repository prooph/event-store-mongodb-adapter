<?php

namespace Prooph\EventStore\Adapter\MongoDb;

use Assert\Assertion;
use Prooph\Common\Messaging\DomainEvent;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Exception\StreamNotFoundException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * EventStore Adapter for MongoDb
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
     * @var \MongoInsertBatch
     */
    protected $insertBatch;

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
     * @var array
     */
    protected $writeConcern;

    /**
     * @param \MongoClient $mongoClient
     * @param string $dbName
     * @param array $writeConcern
     * @param string|null $streamCollectionName
     */
    public function __construct(
        \MongoClient $mongoClient,
        $dbName,
        array $writeConcern = [],
        $streamCollectionName = null
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->mongoClient = $mongoClient;
        $this->dbName      = $dbName;

        if ($streamCollectionName) {
            $this->streamCollectionName = $streamCollectionName;
        }

        $this->writeConcern = array_merge(['w' => 1, 'j' => true], $writeConcern);
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


        if (1 == count($streamEvents)) {
            $eventData = $this->prepareEventData($streamName, reset($streamEvents));
            $this->getCollection()->insert($eventData, $this->writeConcern);
        } else {
            $data = [];

            foreach ($streamEvents as $streamEvent) {
                $data[] = $this->prepareEventData($streamName, $streamEvent);
            }

            $this->getInsertBatch()->add($data);
        }
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
        $this->transactionId = new \MongoId();
    }

    /**
     * Commit transaction
     *
     * @return void
     */
    public function commit()
    {
        $writeConcern = $this->writeConcern;
        $writeConcern['multiple'] = true;

        $this->getCollection()->update(
            [
                'transaction_id' => $this->transactionId
            ],
            [
                '$unset' => [
                    'expire_at' => 1,
                    'transaction_id' => 1
                ]
            ],
            $writeConcern
        );

        $this->transactionId = null;
    }

    /**
     * Rollback transaction
     *
     * @return void
     */
    public function rollback()
    {
        $writeConcern = $this->writeConcern;
        $writeConcern['multiple'] = true;

        $this->getCollection()->remove(
            [
                'transaction_id' => $this->transactionId
            ],
            $writeConcern
        );

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
                'expireAfterSeconds' => 50
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
}
