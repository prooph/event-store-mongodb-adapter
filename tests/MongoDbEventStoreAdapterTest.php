<?php

namespace Prooph\EventStore\Adapter\MongDbTest;

use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Stream\DomainEventMetadataWriter;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStoreTest\Mock\UserCreated;
use Prooph\EventStoreTest\Mock\UsernameChanged;
use Prooph\EventStoreTest\TestCase;

/**
 * Class MongoDbEventStoreAdapterTest
 * @package Prooph\EventStore\Adapter\MongDbTest
 */
class MongoDbEventStoreAdapterTest extends TestCase
{
    /**
     * @var MongoDbEventStoreAdapter
     */
    protected $adapter;

    /**
     * @var \MongoClient
     */
    protected $client;

    protected function setUp()
    {
        $this->client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $this->client->selectDB($dbName)->drop();

        $options = [
            'mongo_client' => $this->client,
            'db_name'      => $dbName
        ];

        $this->adapter = new MongoDbEventStoreAdapter($options);
    }

    protected function tearDown()
    {
        $this->client->selectDB('mongo_adapter_test')->drop();
    }

    /**
     * @test
     */
    public function it_creates_a_stream()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvents = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), array('tag' => 'person'));

        $this->assertEquals(1, count($streamEvents));

        $this->assertEquals($testStream->streamEvents()[0]->uuid()->toString(), $streamEvents[0]->uuid()->toString());
        $this->assertEquals($testStream->streamEvents()[0]->createdAt()->format('Y-m-d\TH:i:s.uO'), $streamEvents[0]->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('Prooph\EventStoreTest\Mock\UserCreated', $streamEvents[0]->messageName());
        $this->assertEquals('contact@prooph.de', $streamEvents[0]->payload()['email']);
        $this->assertEquals(1, $streamEvents[0]->version());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            array('name' => 'John Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), array($streamEvent));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'));

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
    }

    /**
     * @test
     */
    public function it_loads_events_from_min_version_on()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            array('name' => 'John Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent1, 'tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            array('name' => 'Jane Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent2, 'tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), array($streamEvent1, $streamEvent2));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
        $this->assertEquals('John Doe', $stream->streamEvents()[0]->payload()['name']);
        $this->assertEquals('Jane Doe', $stream->streamEvents()[1]->payload()['name']);
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Exception\StreamNotFoundException
     * @expectedExceptionMessage Stream with name Invalid\Stream\Name cannot be found
     */
    public function it_throws_exception_when_no_stream_found()
    {
        $this->adapter->load(new StreamName('Invalid\Stream\Name'));
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Adapter\Exception\ConfigurationException
     * @expectedExceptionMessage Mongo client configuration is missing
     */
    public function it_throws_exception_when_no_mongo_client_set()
    {
        new MongoDbEventStoreAdapter([]);
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Adapter\Exception\ConfigurationException
     * @expectedExceptionMessage MongoClient must be an instance of MongoClient
     */
    public function it_throws_exception_when_invalid_mongo_client_set()
    {
        new MongoDbEventStoreAdapter(['mongo_client' => 'foobar']);
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Adapter\Exception\ConfigurationException
     * @expectedExceptionMessage Mongo database name is missing
     */
    public function it_throws_exception_when_no_db_name_set()
    {
        new MongoDbEventStoreAdapter(['mongo_client' => new \MongoClient()]);
    }

    /**
     * @test
     */
    public function it_creates_custom_collection_name()
    {
        $client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $client->selectDB($dbName)->drop();

        $options = [
            'mongo_client'    => $client,
            'db_name'         => $dbName,
            'collection_name' => 'custom_collection'
        ];

        $this->adapter = new MongoDbEventStoreAdapter($options);

        $this->adapter->create($this->getTestStream());
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Exception\RuntimeException
     */
    public function it_throws_exception_when_empty_stream_created()
    {
        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), []));
    }

    /**
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = UserCreated::with(
            array('name' => 'Max Mustermann', 'email' => 'contact@prooph.de'),
            1
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), [$streamEvent]);
    }
}
