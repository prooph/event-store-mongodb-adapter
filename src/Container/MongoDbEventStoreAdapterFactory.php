<?php

/**
 * /*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014-2018 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\MongoDb\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresMandatoryOptions;
use MongoDB\Client;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\MongoDb\MongoDbEventStoreAdapter;
use Psr\Container\ContainerInterface;

/**
 * Class MongoDbEventStoreAdapterFactory
 * @package Prooph\EventStore\Adapter\MongoDb\Container
 */
final class MongoDbEventStoreAdapterFactory implements RequiresConfig, RequiresMandatoryOptions, ProvidesDefaultOptions
{
    use ConfigurationTrait;

    public function __invoke(ContainerInterface $container): MongoDbEventStoreAdapter
    {
        $config = $container->get('config');
        $config = $this->options($config)['adapter']['options'];

        $mongoClient = isset($config['mongo_connection_alias'])
            ? $container->get($config['mongo_connection_alias'])
            : new Client();

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
            $config['stream_collection_map'],
            $config['disable_transaction_handling']
        );
    }

    public function dimensions(): iterable
    {
        return ['prooph', 'event_store'];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'adapter' => [
                'options' => [
                    'db_name',
                ],
            ],
        ];
    }

    public function defaultOptions(): iterable
    {
        return [
            'adapter' => [
                'options' => [
                    'stream_collection_map' => [],
                    'disable_transaction_handling' => false,
                ],
            ],
        ];
    }
}
