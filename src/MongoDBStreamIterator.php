<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 09/25/15 - 15:17
 */

namespace Prooph\EventStore\Adapter\MongoDb;

use Iterator;
use IteratorIterator;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

/**
 * Class MongoDBStreamIterator
 * @package Prooph\EventStore\Adapter\MongoDb
 */
final class MongoDBStreamIterator implements Iterator
{
    /**
     * @var Manager
     */
    private $manager;

    /**
     * @var string
     */
    private $namespace;

    /**
     * @var Query
     */
    private $query;

    /**
     * @var ReadPreference
     */
    private $readPreference;

    /**
     * @var Generator
     */
    private $innerIterator;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var array
     */
    private $metadata;

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
     * @param Manager $manager
     * @param string $namespace
     * @param Query $query
     * @param ReadPreference $readPreference
     * @param MessageFactory $messageFactory
     * @param array $metadata
     */
    public function __construct(
        Manager $manager,
        $namespace,
        Query $query,
        ReadPreference $readPreference,
        MessageFactory $messageFactory,
        array $metadata
    ) {
        $this->manager = $manager;
        $this->namespace = $namespace;
        $this->query = $query;
        $this->readPreference = $readPreference;
        $this->messageFactory = $messageFactory;
        $this->metadata = $metadata;

        $this->rewind();
    }

    /**
     * @return Message
     */
    public function current()
    {
        $current = $this->innerIterator->current();

        $metadata = [];

        foreach ($current as $key => $value) {
            if (! in_array($key, $this->standardColumns)) {
                $metadata[$key] = $value;
            }
        }

        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $current['created_at'],
            new \DateTimeZone('UTC')
        );

        return $this->messageFactory->createMessageFromArray($current['event_name'], [
            'uuid' => $current['_id'],
            'version' => $current['version'],
            'created_at' => $createdAt,
            'payload' => $current['payload'],
            'metadata' => $metadata
        ]);
    }

    public function next()
    {
        return $this->innerIterator->next();
    }

    public function key()
    {
        return $this->innerIterator->key();
    }

    public function valid()
    {
        return $this->innerIterator->valid();
    }

    /**
     * Rewind
     */
    public function rewind()
    {
        $cursor = $this->manager->executeQuery($this->namespace, $this->query, $this->readPreference);
        $cursor->setTypeMap(['document' => 'array', 'root' => 'array']);
        $this->innerIterator = new IteratorIterator($cursor);
        $this->innerIterator->rewind();
    }
}
