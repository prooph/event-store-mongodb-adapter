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

namespace Prooph\EventStore\Adapter\MongDbTest;

use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStoreTest\Mock\UserCreated;
use Prooph\EventStoreTest\Mock\UsernameChanged;

/**
 * Class MongoDbEventStoreAdapterTest
 * @package Prooph\EventStore\Adapter\MongDbTest
 */
final class MongoDbEventStoreAdapterTest extends TestCase
{
    /**
     * @var MongoDbEventStoreAdapter
     */
    private $adapter;

    /**
     * @var \MongoClient
     */
    private $client;

    protected function setUp()
    {
        $this->client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $this->client->selectDB($dbName)->drop();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName
        );
    }

    protected function tearDown()
    {
        if (null !== $this->client) {
            $this->client->selectDB('mongo_adapter_test')->drop();
        }
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

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

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

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            2
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), [$streamEvent1, $streamEvent2]);

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
        $this->assertEquals('John Doe', $stream->streamEvents()[0]->payload()['name']);
        $this->assertEquals('Jane Doe', $stream->streamEvents()[1]->payload()['name']);
    }

    /**
     * @test
     * @expectedException Assert\InvalidArgumentException
     * @expectedExceptionMessage Mongo database name is missing
     */
    public function it_throws_exception_when_no_db_name_set()
    {
        new MongoDbEventStoreAdapter(new FQCNMessageFactory(), new NoOpMessageConverter(), new \MongoClient(), null);
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
     * @expectedException Assert\InvalidArgumentException
     * @expectedExceptionMessage Transaction timeout must be a positive integer
     */
    public function it_throws_exception_when_invalid_transaction_timeout_given()
    {
        new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new \MongoClient(),
            'mongo_adapter_test',
            null,
            'invalid'
        );
    }

    /**
     * @test
     */
    public function it_accepts_custom_transaction_timeout()
    {
        new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new \MongoClient(),
            'mongo_adapter_test',
            null,
            10
        );
    }

    /**
     * @test
     */
    public function it_can_rollback_transaction()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->rollback();

        $result = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertEmpty($result);
    }

    /**
     * @test
     */
    public function it_rolls_back_transaction_after_timeout()
    {
        $this->client = new \MongoClient();
        $dbName = 'mongo_adapter_test';

        $this->client->selectDB($dbName)->drop();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName,
            null,
            3
        );

        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        sleep(3);

        $result = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertEmpty($result);
    }

    /**
     * @test
     * @expectedException RuntimeException
     * @expectedExceptionMessage Transaction already started
     */
    public function it_throws_exception_when_second_transaction_started()
    {
        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
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

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), [$streamEvent]);
    }
}
