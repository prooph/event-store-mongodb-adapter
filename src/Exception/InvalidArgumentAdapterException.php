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

namespace Prooph\EventStore\Adapter\MongoDb\Exception;

use InvalidArgumentException;
use Prooph\EventStore\Adapter\Exception\AdapterException;

class InvalidArgumentAdapterException extends InvalidArgumentException implements MongoDbEventStoreAdapterException, AdapterException
{
}
