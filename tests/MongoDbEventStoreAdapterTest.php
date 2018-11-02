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

namespace ProophTest\EventStore\Adapter\MongoDb;

use MongoDB\Client;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\Exception\RuntimeAdapterException;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Exception\ConcurrencyException;
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
     * @var Client
     */
    private $client;

    protected function setUp()
    {
        $this->client = TestUtil::getConnection();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            TestUtil::getDatabaseName()
        );
    }

    protected function tearDown()
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_creates_a_stream(): void
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

        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
        $this->assertEquals(['tag' => 'person'], $event->metadata());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream(): void
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
    public function it_replays(): void
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
        $events = [];
        foreach ($streamEvents as $event) {
            $count++;
            $events[] = $event;
        }
        $this->assertEquals(2, $count);

        $event = $events[0];

        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());

        $event = $events[1];

        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_from_specific_date(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        \sleep(1);

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

        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_events_of_two_aggregates_in_a_single_stream_in_correct_order(): void
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

        \sleep(1);

        $secondUserEvent = UserCreated::with(
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
            3
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
    public function it_loads_events_from_min_version_on(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(0, $stream->streamEvents()->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(1, $stream->streamEvents()->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());
    }

    /**
     * @test
     */
    public function it_loads_events_in_transaction(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $this->adapter->beginTransaction();
        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);
        // it must be committed, otherwise it hangs for a minute
        $this->adapter->commit();

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(0, $stream->streamEvents()->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertTrue($stream->streamEvents()->valid());
        $event = $stream->streamEvents()->current();
        $this->assertEquals(1, $stream->streamEvents()->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());
    }

    /**
     * @test
     * @expectedException \Prooph\EventStore\Exception\ConcurrencyException
     */
    public function it_fails_to_write_with_duplicate_aggregate_id_and_version(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     * @expectedException Assert\InvalidArgumentException
     * @expectedExceptionMessage Mongo database name is missing
     */
    public function it_throws_exception_when_no_db_name_set(): void
    {
        new MongoDbEventStoreAdapter(new FQCNMessageFactory(), new NoOpMessageConverter(), new Client(), '');
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Exception\RuntimeException
     */
    public function it_throws_exception_when_empty_stream_created(): void
    {
        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([])));
    }

    /**
     * @test
     */
    public function it_accepts_disable_transaction_handling(): void
    {
        $adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new Client(),
            'mongo_adapter_test',
            [],
            true
        );
        $adapter->beginTransaction();
        $adapter->commit();
        $adapter->rollback();
        $this->assertTrue(true);
    }

    /**
     * @test
     */
    public function it_can_rollback_transaction(): void
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
     * @expectedException RuntimeException
     * @expectedExceptionMessage Transaction already started
     */
    public function it_throws_exception_when_second_transaction_started(): void
    {
        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
    }

    /**
     * @test
     */
    public function it_uses_custom_stream_collection_map(): void
    {
        $dbName = TestUtil::getDatabaseName();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName,
            [
                'Prooph\Model\User' => 'test_collection_name',
            ]
        );

        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $collectionContent = $this->client->selectCollection($dbName, 'test_collection_name')->find([]);

        $this->assertCount(1, $collectionContent->toArray());
    }

    /**
     * @test
     */
    public function it_throws_exception_when_trying_to_write_to_different_streams_in_one_transaction_if_they_not_exists(): void
    {
        $dbName = TestUtil::getDatabaseName();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName,
            [
                'Prooph\Model\User' => 'test_collection_name',
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

        $this->expectException(RuntimeAdapterException::class);
        $this->expectExceptionMessage('Could not write to event stream');

        $this->adapter->appendTo(new StreamName('another_one'), new \ArrayIterator([$streamEvent]));

        $this->adapter->commit();
    }

    /**
     * @test
     */
    public function it_writes_to_different_streams_in_one_transaction_if_they_exists(): void
    {
        $dbName = TestUtil::getDatabaseName();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName,
            [
                'Prooph\Model\User' => 'test_collection_name',
            ]
        );

        $testStream = $this->getTestStream();

        $anotherTestStream = new Stream(
            new StreamName('another_test_stream'),
            new \ArrayIterator([UserCreated::with(['name' => 'Another Max Mustermann'], 1)])
        );

        $this->adapter->create($testStream);
        $this->adapter->create($anotherTestStream);

        $this->adapter->beginTransaction();

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            2
        );
        $anotherStreamEvent = UserCreated::with(
            ['name' => 'Another Second Max Mustermann'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $anotherStreamEvent = $anotherStreamEvent->withAddedMetadata('tag', 'anotherPerson');

        $this->adapter->appendTo($testStream->streamName(), new \ArrayIterator([$streamEvent]));
        $this->adapter->appendTo($anotherTestStream->streamName(), new \ArrayIterator([$anotherStreamEvent]));

        $this->adapter->commit();

        $this->assertEquals(2, $this->client->selectCollection($dbName, 'test_collection_name')->countDocuments());
        $this->assertEquals(2, $this->client->selectCollection($dbName, 'another_test_stream')->countDocuments());
    }

    /**
     * @test
     */
    public function it_throws_concurrency_exception_on_duplicate_key_error(): void
    {
        $dbName = TestUtil::getDatabaseName();

        $this->adapter = new MongoDbEventStoreAdapter(
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            $this->client,
            $dbName,
            [
                'Prooph\Model\User' => 'test_collection_name',
            ]
        );

        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->expectException(ConcurrencyException::class);
        $this->expectExceptionMessage('At least one event');

        $this->adapter->appendTo($testStream->streamName(), $testStream->streamEvents());

        $this->adapter->commit();
    }

    /**
     * @test
     */
    public function it_can_commit_empty_transaction(): void
    {
        $this->adapter->beginTransaction();
        $this->adapter->commit();
        $this->assertTrue(true);
    }

    /**
     * @test
     */
    public function it_can_rollback_empty_transaction(): void
    {
        $this->adapter->beginTransaction();
        $this->adapter->rollback();
        $this->assertTrue(true);
    }

    /**
     * @return Stream
     */
    private function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
