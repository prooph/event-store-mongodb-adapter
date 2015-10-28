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

use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

/**
 * Class MongoDbEventStoreAdapterTest
 * @package ProophTest\EventStore\Adapter\MongoDb
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

        $streamEvents = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $testEvent = $testStream->streamEvents()->current();
        $event = $streamEvents->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
        $this->assertEquals(['tag' => 'person'], $event->metadata());
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

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'));

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $count = 0;
        $lastEvent = null;
        foreach ($stream->streamEvents()  as $event) {
            $count++;
            $lastEvent = $event;
        }
        $this->assertEquals(2, $count);
        $this->assertInstanceOf(UsernameChanged::class, $lastEvent);
        $messageConverter = new NoOpMessageConverter();

        $streamEventData = $messageConverter->convertToArray($streamEvent);
        $lastEventData = $messageConverter->convertToArray($lastEvent);

        $this->assertEquals($streamEventData, $lastEventData);
    }

    /**
     * @test
     */
    public function it_replays()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), null, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(2, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $testEvent = $testStream->streamEvents()->current();
        $event = $streamEvents->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());

        $streamEvents->next();
        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_from_specific_date()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        sleep(1);

        $since = new \DateTime('now', new \DateTimeZone('UTC'));

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), $since, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_events_of_two_aggregates_in_a_single_stream_in_correct_order()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        sleep(1);

        $secondUserEvent = UserCreated::with(
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
            1
        );

        $secondUserEvent = $secondUserEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$secondUserEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), null, ['tag' => 'person']);


        $replayedPayloads = [];
        foreach ($streamEvents as $event) {
            $replayedPayloads[] = $event->payload();
        }

        $expectedPayloads = [
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            ['name' => 'John Doe'],
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
        ];

        $this->assertEquals($expectedPayloads, $replayedPayloads);
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

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertNotEmpty($stream->streamEvents()->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertNotEmpty($stream->streamEvents()->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());
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
        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([])));
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

        $result = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertFalse($result->valid());
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

        $result = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertFalse($result->valid());
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
     * @test
     */
    public function it_uses_custom_stream_collection_map()
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
            3,
            [
                'Prooph\Model\User' => 'test_collection_name'
            ]
        );

        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $collectionContent = $this->client->selectCollection($dbName, 'test_collection_name')->find([]);

        $this->assertEquals(1, count($collectionContent));
    }

    /**
     * @test
     * @expectedException RuntimeException
     * @expectedExceptionMessage Cannot write to different stream streams in one transaction
     */
    public function it_throws_exception_when_trying_to_write_to_different_streams_in_one_transaction()
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
            3,
            [
                'Prooph\Model\User' => 'test_collection_name'
            ]
        );

        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('another_one'), new \ArrayIterator([$streamEvent]));

        $this->adapter->commit();
    }

    /**
     * @test
     */
    public function it_can_commit_empty_transaction()
    {
        $this->adapter->beginTransaction();
        $this->adapter->commit();
    }

    /**
     * @test
     */
    public function it_can_rollback_empty_transaction()
    {
        $this->adapter->beginTransaction();
        $this->adapter->rollback();
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

        return new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
