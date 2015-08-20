<?php

namespace Prooph\EventStore\Adapter\MongDbTest\Service;

use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Adapter\MongoDb\Service\MongoDbEventStoreAdapterFactory;

/**
 * Class MongoDbEventStoreAdapterFactoryTest
 * @package Prooph\EventStore\Adapter\MongDbTest\Service
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
        $collectionName = 'stream_collection';

        $config = [];
        $config['prooph']['event_store']['adapter']['options'] = [
            'mongo_connection_alias' => 'mongo_connection',
            'db_name' => $dbName,
            'collection_name' => $collectionName
        ];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));
        $mock->expects($this->at(1))->method('get')->with('mongo_connection')->will($this->returnValue($client));

        $factory = new MongoDbEventStoreAdapterFactory();
        $adapter = $factory($mock);

        $this->assertInstanceOf(MongoDbEventStoreAdapter::class, $adapter);
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Configuration\Exception\ConfigurationException
     * @expectedExceptionMessage Mongo database name is missing
     */
    public function it_throws_exception_if_db_name_is_missing()
    {
        $config = [];
        $config['prooph']['event_store']['adapter']['options'] = [
        ];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));

        $factory = new MongoDbEventStoreAdapterFactory();
        $factory($mock);
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Configuration\Exception\ConfigurationException
     * @expectedExceptionMessage Missing adapter configuration in proophessor event_store configuration
     */
    public function it_throws_exception_if_adapter_config_missing()
    {
        $config = [];
        $config['prooph']['event_store'] = [];

        $mock = $this->getMockForAbstractClass(ContainerInterface::class);
        $mock->expects($this->at(0))->method('get')->with('config')->will($this->returnValue($config));

        $factory = new MongoDbEventStoreAdapterFactory();
        $factory($mock);
    }
}
