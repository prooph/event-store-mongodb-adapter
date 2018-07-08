<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 08/08/15 - 20:32
 */

namespace ProophTest\EventStore\Adapter\MongoDb;

/**
 * Class MongoDbEventStoreAdapterTest
 * @package ProophTest\EventStore\Adapter\MongoDb
 */
final class MongoDbEventStoreAdapterIsolatedTest extends AbstractMongoDbEventStoreAdapterTest
{
    public function disableIsolated()
    {
        return false;
    }
}
