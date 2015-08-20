<?php

namespace Prooph\EventStore\Adapter\MongDbTest;

use PHPUnit_Framework_TestCase as TestCase;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Stream\DomainEventMetadataWriter;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStoreTest\Mock\UserCreated;
use Prooph\EventStoreTest\Mock\UsernameChanged;

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

        $this->adapter = new MongoDbEventStoreAdapter($this->client, $dbName);
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

        $streamEvents = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

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
            ['name' => 'John Doe'],
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), [$streamEvent]);

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
            ['name' => 'John Doe'],
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent1, 'tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent2, 'tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), [$streamEvent1, $streamEvent2]);

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
     * @expectedExceptionMessage Mongo database name is missing
     */
    public function it_throws_exception_when_no_db_name_set()
    {
        new MongoDbEventStoreAdapter(new \MongoClient(), null);
    }

    /**
     * @test
     */
    public function it_creates_custom_collection_name()
    {
        $client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $client->selectDB($dbName)->drop();

        $this->adapter = new MongoDbEventStoreAdapter($client, $dbName, 'custom_collection');

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
     * @test
     * @expectedException Prooph\EventStore\Exception\StreamNotFoundException
     * @expectedExceptionMessage Stream with name Prooph\Model\User cannot be found
     */
    public function it_can_rollback_transaction()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->rollback();

        $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);
    }

    /**
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), [$streamEvent]);
    }
}
