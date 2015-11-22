<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 08/20/15 - 17:51
 */

namespace ProophTest\EventStore\Adapter\MongoDb\Service;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Adapter\MongoDb\Container\MongoDbEventStoreAdapterFactory;

/**
 * Class MongoDbEventStoreAdapterFactoryTest
 * @package ProophTest\EventStore\Adapter\MongoDb\Service
 */
class MongoDbEventStoreAdapterFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter()
    {
        $client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $config = [];
        $config['prooph']['event_store']['adapter']['options'] = [
            'mongo_connection_alias' => 'mongo_connection',
            'db_name' => $dbName,
        ];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));
        $mock->expects($this->at(1))->method('get')->with('mongo_connection')->will($this->returnValue($client));

        $factory = new MongoDbEventStoreAdapterFactory();
        $adapter = $factory($mock);

        $this->assertInstanceOf(MongoDbEventStoreAdapter::class, $adapter);
    }
}
