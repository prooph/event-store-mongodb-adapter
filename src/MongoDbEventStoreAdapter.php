<?php

namespace Prooph\EventStore\Adapter\MongoDb;

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
     * @param  array $configuration
     * @throws ConfigurationException
     */
    public function __construct(array $configuration)
    {
        if (!isset($configuration['mongo_client'])) {
            throw new ConfigurationException('Mongo client configuration is missing');
        }

        if (!$configuration['mongo_client'] instanceof \MongoClient) {
            throw new ConfigurationException('MongoClient must be an instance of MongoClient');
        }

        if (!isset($configuration['db_name'])) {
            throw new ConfigurationException('Mongo database name is missing');
        }

        if (isset($configuration['collection_name'])) {
            $this->streamCollectionName = $configuration['collection_name'];
        }

        $this->mongoClient = $configuration['mongo_client'];
        $this->dbName      = $configuration['db_name'];
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
        $collection = $this->getCollection();

        if (1 == count($streamEvents)) {
            $eventData = $this->prepareEventData($streamName, reset($streamEvents));
            $collection->insert($eventData);
        } else {
            $data = [];

            foreach ($streamEvents as $streamEvent) {
                $data[] = $this->prepareEventData($streamName, $streamEvent);
            }

            $collection->batchInsert($data);
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
            [
                'multiple' => true
            ]
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
        $this->getCollection()->remove(
            [
                'transaction_id' => $this->transactionId
            ],
            [
                'multiple' => true
            ]
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
        $collection = $this->mongoClient->selectCollection($this->dbName, $this->streamCollectionName);
        $collection->setReadPreference(\MongoClient::RP_PRIMARY);

        return $collection;
    }
}
