<?php

/**
 * /*
 * This file is part of prooph/event-store-mongodb-adapter.
 * (c) 2014-2018 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\MongoDb;

use Assert\Assertion;
use DateTimeInterface;
use Generator;
use Iterator;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Session;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Adapter\MongoDb\Exception\RuntimeAdapterException;
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
    private const SORT_ASCENDING = 1;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    /**
     * @var Client
     */
    private $mongoClient;

    /**
     * @var string
     */
    private $dbName;

    /**
     * The transaction id, if currently in transaction, otherwise null
     *
     * @var Session|null
     */
    private $session;

    /**
     * Custom sourceType to collection mapping
     *
     * @var array
     */
    private $streamCollectionMap = [];

    /**
     * @var bool
     */
    private $disableTransactionHandling;

    /**
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param Client $mongoClient
     * @param string $dbName
     * @param array $streamCollectionMap
     * @param bool $disableTransactionHandling
     * @throws \Assert\AssertionFailedException
     */
    public function __construct(
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        Client $mongoClient,
        string $dbName,
        array $streamCollectionMap = [],
        bool $disableTransactionHandling = false
    ) {
        Assertion::minLength($dbName, 1, 'Mongo database name is missing');

        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->mongoClient = $mongoClient;
        $this->dbName = $dbName;
        $this->streamCollectionMap = $streamCollectionMap;
        $this->disableTransactionHandling = $disableTransactionHandling;
    }

    /**
     * @param Stream $stream
     * @throws RuntimeException If creation of stream fails
     * @return void
     */
    public function create(Stream $stream)
    {
        if (! $stream->streamEvents()->valid()) {
            throw new RuntimeException(
                \sprintf(
                    'Cannot create empty stream %s. %s requires at least one event to extract metadata information',
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
        $options = [];

        if (! $this->disableTransactionHandling) {
            $options = [
                'session' => $this->session,
            ];
        }

        try {
            foreach ($streamEvents as $streamEvent) {
                $this->getCollection($streamName)->insertOne(
                    $this->prepareEventData($streamEvent),
                    $options
                );
            }
        } catch (\MongoDB\Driver\Exception\BulkWriteException $e) {
            $code = isset($e->getWriteResult()->getWriteErrors()[0]) ?
                $e->getWriteResult()->getWriteErrors()[0]->getCode()
                : $e->getCode();

            if (\in_array($code, [11000, 11001, 12582], true)) {
                throw new ConcurrencyException('At least one event with same version exists already', 0, $e);
            }
            throw new RuntimeAdapterException('Could not write to event stream ' . $streamName, $e->getCode(), $e);
        }
    }

    private function prepareEventData(Message $e): array
    {
        $eventArr = $this->messageConverter->convertToArray($e);

        $eventData = [
            '_id' => $eventArr['uuid'],
            'version' => $eventArr['version'],
            'event_name' => $eventArr['message_name'],
            'payload' => $eventArr['payload'],
            'created_at' => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        foreach ($eventArr['metadata'] as $key => $value) {
            $eventData[$key] = (string) $value;
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

        $options = [
            'sort' => ['version' => self::SORT_ASCENDING],
        ];

        if (! $this->disableTransactionHandling) {
            $options = [
                'session' => $this->session,
            ];
        }

        $cursor = $collection->find($query, $options);

        return new MongoDbStreamIterator($this->yieldCursor($cursor), $this->messageFactory, $metadata);
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

        $options = [
            'sort' => [
                'created_at' => self::SORT_ASCENDING,
                'version' => self::SORT_ASCENDING,
            ],
        ];

        if (! $this->disableTransactionHandling) {
            $options = [
                'session' => $this->session,
            ];
        }

        $cursor = $collection->find($query, $options);

        return new MongoDbStreamIterator($this->yieldCursor($cursor), $this->messageFactory, $metadata);
    }

    private function yieldCursor(Cursor $cursor): Generator
    {
        foreach ($cursor as $item) {
            yield $item;
        }
    }

    /**
     * Begin transaction
     *
     * @return void
     */
    public function beginTransaction()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null !== $this->session) {
            throw new \RuntimeException('Transaction already started');
        }
        $this->session = $this->mongoClient->startSession();
        $this->session->startTransaction([
            'readConcern' => new \MongoDB\Driver\ReadConcern('snapshot'),
            'writeConcern' => new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY),
        ]);
    }

    /**
     * Commit transaction
     *
     * @return void
     */
    public function commit()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null === $this->session) {
            throw new RuntimeAdapterException('Transaction not started');
        }

        $this->session->commitTransaction();
        $this->session->endSession();
        $this->session = null;
    }

    /**
     * Rollback transaction
     *
     * @return void
     */
    public function rollback()
    {
        if ($this->disableTransactionHandling) {
            return;
        }

        if (null === $this->session) {
            throw new RuntimeAdapterException('Transaction not started');
        }
        $this->session->abortTransaction();
        $this->session->endSession();
        $this->session = null;
    }

    private function createIndexes(StreamName $streamName): void
    {
        $this->getCollection($streamName)
            ->createIndexes(
                [
                    [
                        'key' => ['aggregate_id' => 1, 'version' => 1],
                        'unique' => true,
                        'name' => 'aggregate_id_version',
                        'background' => true,
                    ],
                    [
                        'key' => ['created_at' => 1, 'version' => 1],
                        'name' => 'created_at_version',
                        'background' => true,
                    ],
                ]
            );
    }

    /**
     * Get mongo db stream collection
     *
     * @param StreamName $streamName
     * @return Collection
     */
    private function getCollection(StreamName $streamName): Collection
    {
        $collection = $this->mongoClient->selectCollection($this->dbName, $this->getCollectionName($streamName));

        return $collection;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getCollectionName(StreamName $streamName): string
    {
        if (isset($this->streamCollectionMap[$streamName->toString()])) {
            $collectionName = $this->streamCollectionMap[$streamName->toString()];
        } else {
            $collectionName = \strtolower($this->getShortStreamName($streamName));
            if (\strpos($collectionName, '_stream') === false) {
                $collectionName .= '_stream';
            }
        }

        return $collectionName;
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getShortStreamName(StreamName $streamName): string
    {
        $streamName = \str_replace('-', '_', $streamName->toString());

        return \implode('', \array_slice(\explode('\\', $streamName), -1));
    }
}
