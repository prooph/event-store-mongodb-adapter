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

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;

/**
 * Class MongoDbEventStoreAdapterFactory
 * @package Prooph\EventStore\Adapter\MongoDb\Container
 */
final class MongoDbEventStoreAdapterFactory implements RequiresConfig, RequiresMandatoryOptions, ProvidesDefaultOptions
{
    use ConfigurationTrait;

    /**
     * @param ContainerInterface $container
     * @return MongoDbEventStoreAdapter
     */
    public function __invoke(ContainerInterface $container)
    {
        $config = $container->get('config');
        $config = $this->options($config)['adapter']['options'];

        $mongoClient = isset($config['mongo_connection_alias'])
            ? $container->get($config['mongo_connection_alias'])
            : new \MongoClient();

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();

        return new MongoDbEventStoreAdapter(
            $messageFactory,
            $messageConverter,
            $mongoClient,
            $config['db_name'],
            $config['write_concern'],
            $config['transaction_timeout'],
            $config['stream_collection_map']
        );
    }

    /**
     * @inheritdoc
     */
    public function vendorName()
    {
        return 'prooph';
    }
    /**
     * @inheritdoc
     */
    public function packageName()
    {
        return 'event_store';
    }
    /**
     * @inheritdoc
     */
    public function mandatoryOptions()
    {
        return [
            'adapter' => [
                'options' => [
                    'db_name'
                ]
            ]
        ];
    }
    /**
     * @inheritdoc
     */
    public function defaultOptions()
    {
        return [
            'adapter' => [
                'options' => [
                    'stream_collection_map' => [],
                    'transaction_timeout' => null,
                    'write_concern' => [
                        'w' => 1,
                        'j' => true
                    ],
                ]
            ]
        ];
    }
}
