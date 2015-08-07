<?php

namespace Prooph\EventStore\Adapter\MongoDb;

use Prooph\Common\Messaging\DomainEvent;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * EventStore Adapter for MongoDb
 */
class MongoDbEventStoreAdapter implements Adapter
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
     * Custom sourceType to table mapping
     *
     * @var array
     */
    protected $streamTableMap = [];

    /**
     * @var array
     */
    protected $standardColumns = ['_id', 'event_name', 'event_class', 'created_at', 'payload', 'version'];

    /**
     * @param  array $configuration
     * @throws \Prooph\EventStore\Adapter\Exception\ConfigurationException
     */
    public function __construct(array $configuration)
    {
        if (!isset($configuration['mongo_client'])) {
            throw new ConfigurationException('Mongo client configuration is missing');
        }

        if (!isset($configuration['db_name'])) {
            throw new ConfigurationException('Mongo database name is missing');
        }

        if (isset($configuration['stream_table_map'])) {
            $this->streamTableMap = $configuration['stream_table_map'];
        }

        $this->mongoClient = $configuration['mongo_client'];
        $this->dbName      = $configuration['db_name'];
    }

    /**
     * @param Stream $stream
     * @throws \Prooph\EventStore\Exception\RuntimeException If creation of stream fails
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

        $firstEvent = $stream->streamEvents()[0];

        $this->createIndexesFor($stream->streamName(), $firstEvent->metadata());

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param DomainEvent[] $streamEvents
     * @throws \Prooph\EventStore\Exception\StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $streamName, array $streamEvents)
    {
        $data = [];

        foreach ($streamEvents as $e) {
            $eventData = array(
                '_id' => $e->uuid()->toString(),
                'version' => $e->version(),
                'event_name' => $e->messageName(),
                'event_class' => get_class($e),
                'payload' => $e->payload(),
                'created_at' => new \MongoDate($e->createdAt()->getTimestamp()),
            );

            foreach ($e->metadata() as $key => $value) {
                $eventData[$key] = (string)$value;
            }

            $data[] = $eventData;
        }

        $collection = $this->getCollection($streamName);

        $collection->batchInsert($data, ['safe' => true]);
    }

    /**
     * @param StreamName $streamName
     * @param null|int $minVersion
     * @return Stream|null
     */
    public function load(StreamName $streamName, $minVersion = null)
    {
        $events = $this->loadEventsByMetadataFrom($streamName, array(), $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return DomainEvent[]
     */
    public function loadEventsByMetadataFrom(StreamName $streamName, array $metadata, $minVersion = null)
    {
        $collection = $this->getCollection($streamName);

        if (null !== $minVersion) {
            $metadata['version'] = ['$gt' => [$minVersion]];
        }

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

            $events[] = $eventClass::fromArray(
                [
                    'uuid' => $eventData['_id'],
                    'name' => $eventData['event_name'],
                    'version' => (int) $eventData['version'],
                    'created_at' => $eventData['created_at']->toDateTime()->format(\DateTime::ISO8601),
                    'payload' => $eventData['payload'],
                    'metadata' => $metadata
                ]
            );
        }

        return $events;
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @return void
     */
    protected function createIndexesFor(StreamName $streamName, array $metadata)
    {
        $collection = $this->getCollection($streamName);

        $collection->createIndex(['_id' => 1], ['unique' => true]);
    }

    /**
     * Get table name for given stream name
     *
     * @param StreamName $streamName
     * @return \MongoCollection
     */
    protected function getCollection(StreamName $streamName)
    {
        if (isset($this->streamTableMap[$streamName->toString()])) {
            $collectionName = $this->streamTableMap[$streamName->toString()];
        } else {
            $collectionName = strtolower($this->getShortStreamName($streamName));

            if (strpos($collectionName, "_stream") === false) {
                $collectionName.= "_stream";
            }
        }

        $collection = $this->mongoClient->selectCollection($this->dbName, $collectionName);
        $collection->setReadPreference(\MongoClient::RP_PRIMARY);

        return $collection;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    protected function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return join('', array_slice(explode('\\', $streamName), -1));
    }
}
