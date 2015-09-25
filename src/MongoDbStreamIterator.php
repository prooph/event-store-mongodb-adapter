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
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;

/**
 * Class MongoDbStreamIterator
 * @package Prooph\EventStore\Adapter\MongoDb
 */
final class MongoDbStreamIterator implements Iterator
{
    /**
     * @var Iterator
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
     * @param Iterator $iterator
     * @param MessageFactory $messageFactory
     * @param array $metadata
     */
    public function __construct(Iterator $iterator, MessageFactory $messageFactory, array $metadata)
    {
        $this->innerIterator = $iterator;
        $this->messageFactory = $messageFactory;
        $this->metadata = $metadata;

        $iterator->rewind();
    }

    /**
     * @return Message
     */
    public function current()
    {
        $current = $this->innerIterator->current();

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
            'version' => (int) $current['version'],
            'created_at' => $createdAt,
            'payload' => $current['payload'],
            'metadata' => $this->metadata
        ]);
    }

    /**
     * Next
     */
    public function next()
    {
        $this->innerIterator->next();
    }

    /**
     * @return mixed
     */
    public function key()
    {
        return $this->innerIterator->key();
    }

    /**
     * @return bool
     */
    public function valid()
    {
        return $this->innerIterator->valid();
    }

    /**
     * Rewind
     */
    public function rewind()
    {
        $this->innerIterator->rewind();
    }
}
