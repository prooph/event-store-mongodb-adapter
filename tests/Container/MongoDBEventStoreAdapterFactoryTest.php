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
use MongoDB\Driver\Manager;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\MongoDb\MongoDBEventStoreAdapter;
use Prooph\EventStore\Adapter\MongoDb\Container\MongoDBEventStoreAdapterFactory;

/**
 * Class MongoDBEventStoreAdapterFactoryTest
 * @package ProophTest\EventStore\Adapter\MongoDb\Service
 */
class MongoDBEventStoreAdapterFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_adapter()
    {
        $manager = new Manager('mongodb://localhost:27017');
        $dbName = 'mongo_adapter_test';

        $config = [];
        $config['prooph']['event_store']['adapter']['options'] = [
            'mongo_manager' => 'mongo_manager',
            'db_name' => $dbName,
        ];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));
        $mock->expects($this->at(1))->method('get')->with('mongo_manager')->will($this->returnValue($manager));

        $factory = new MongoDBEventStoreAdapterFactory();
        $adapter = $factory($mock);

        $this->assertInstanceOf(MongoDBEventStoreAdapter::class, $adapter);
    }
}
