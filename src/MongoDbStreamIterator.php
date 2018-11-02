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

use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

/**
 * Class MongoDbStreamIterator
 * @package Prooph\EventStore\Adapter\MongoDb
 */
final class MongoDbStreamIterator implements Iterator
{
    /**
     * @var \Generator
     */
    private $iterator;

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
        'expire_at',
    ];

    /**
     * @param \Generator $iterator
     * @param MessageFactory $messageFactory
     * @param array $metadata
     */
    public function __construct(\Generator $iterator, MessageFactory $messageFactory, array $metadata)
    {
        $this->iterator = $iterator;
        $this->messageFactory = $messageFactory;
        $this->metadata = $metadata;
    }

    /**
     * @return Message
     */
    public function current()
    {
        $current = $this->iterator->current();

        $metadata = [];

        foreach ($current as $key => $value) {
            if (! \in_array($key, $this->standardColumns, true)) {
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
            'version' => (int) $current['version'],
            'created_at' => $createdAt,
            'payload' => $current['payload'],
            'metadata' => $metadata,
        ]);
    }

    /**
     * Next
     */
    public function next()
    {
        $this->iterator->next();
    }

    /**
     * @return mixed
     */
    public function key()
    {
        return $this->iterator->key();
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return $this->iterator->valid();
    }

    /**
     * Rewind
     */
    public function rewind()
    {
        $this->iterator->rewind();
    }
}
