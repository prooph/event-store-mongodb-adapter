<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 08/20/15 - 17:13
 */

namespace Prooph\EventStore\Adapter\MongoDb\Container;

use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Exception\ConfigurationException;

/**
 * Class MongoDbEventStoreAdapterFactory
 * @package Prooph\EventStore\Adapter\MongoDb\Container
 */
final class MongoDbEventStoreAdapterFactory
{
    /**
     * @param ContainerInterface $container
     * @return MongoDbEventStoreAdapter
     */
    public function __invoke(ContainerInterface $container)
    {
        $config = $container->get('config');

        if (!isset($config['prooph']['event_store']['adapter'])) {
            throw ConfigurationException::configurationError(
                'Missing adapter configuration in prooph event_store configuration'
            );
        }

        $adapterOptions = isset($config['prooph']['event_store']['adapter']['options'])
            ? $config['prooph']['event_store']['adapter']['options']
            : [];

        $mongoClient = isset($adapterOptions['mongo_connection_alias'])
            ? $container->get($adapterOptions['mongo_connection_alias'])
            : new \MongoClient();

        if (!isset($adapterOptions['db_name'])) {
            throw ConfigurationException::configurationError(
                'Mongo database name is missing
                '
            );

        }

        $dbName = $adapterOptions['db_name'];

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();


        $writeConcern = isset($adapterOptions['write_concern']) ? $adapterOptions['write_concern'] : [];

        $streamCollectionName = isset($adapterOptions['collection_name']) ? $adapterOptions['collection_name'] : null;

        $timeout = isset($adapterOptions['transaction_timeout']) ? $adapterOptions['transaction_timeout'] : null;

        return new MongoDbEventStoreAdapter(
            $messageFactory,
            $messageConverter,
            $mongoClient,
            $dbName,
            $writeConcern,
            $streamCollectionName,
            $timeout
        );
    }
}
