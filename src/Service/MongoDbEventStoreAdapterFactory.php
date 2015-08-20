<?php

namespace Prooph\EventStore\Adapter\MongoDb\Service;

use Interop\Container\ContainerInterface;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Prooph\EventStore\Configuration\Exception\ConfigurationException;

/**
 * Class MongoDbEventStoreAdapterFactory
 * @package Prooph\EventStore\Adapter\MongoDb\Service
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
                'Missing adapter configuration in proophessor event_store configuration'
            );
        }

        $adapterOptions = isset($config['prooph']['event_store']['adapter']['options'])
            ? $config['proophessor']['event_store']['adapter']['options']
            : [];

        $mongoClient = isset($adapterOptions['mongo_connection_alias'])
            ? $container->get($adapterOptions['mongo_connection_alias'])
            : new \MongoClient();

        if (!isset($adapterOptions['db_name'])) {
            throw ConfigurationException::configurationError(
                'Mongo database name is missing'
            );
        }

        $dbName = $adapterOptions['db_name'];

        $streamCollectionName = isset($adapterOptions['collection_name']) ? $adapterOptions['collection_name'] : null;

        return new MongoDbEventStoreAdapter($mongoClient, $dbName, $streamCollectionName);
    }
}
