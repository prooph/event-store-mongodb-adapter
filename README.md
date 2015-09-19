# event-store-mongodb-adapter

[![Build Status](https://travis-ci.org/prooph/event-store-mongodb-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-mongodb-adapter)
[![Coverage Status](https://coveralls.io/repos/prooph/event-store-mongodb-adapter/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/event-store-mongodb-adapter?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

MongoDB Adapter for [ProophEventStore](https://github.com/prooph/event-store)

Requirements
------------

- MongoDB >= 2.6.0
- PHP Mongo Extension >= 1.5.0

Write concern
-------------

This adapter uses a transaction timeout of 50 secs by default.

The default write concern for this adapater is acknowledged and journaled (['w' => 1, 'j' => true]).

It's possible to change both values by injecting them into the constructor or by using the factory.

Considerations
--------------

This adapter does not use the MongoDB ObjectId for its primary key, instead a UUID (string) is used.

We recommend the AggregateStreamStrategy as the best strategy to use with this adapter.

This adapter uses die `$isolated` operator to achieve transaction safety for a single collection.
Keep in mind, that `$isolated` does not work with sharded clusters. Therefore it's not safe to use this adapter
in a shared cluster environment, as MongoDB can't guarantee transaction safety.
