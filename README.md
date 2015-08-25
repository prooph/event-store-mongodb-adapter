# event-store-mongodb-adapter

[![Build Status](https://travis-ci.org/prooph/event-store-mongodb-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-mongodb-adapter)
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

Sharding
--------

This adapter does not use the MongoDB ObjectId for its primary key, instead a UUID (string) is used.

When using MongoDB's sharding feature, simply set the "_id" as shard key.

For more information about sharding in MongoDB check the MongoDB manual.
 