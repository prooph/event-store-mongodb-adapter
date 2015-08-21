# event-store-mongodb-adapter

[![Build Status](https://travis-ci.org/prooph/event-store-mongodb-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-mongodb-adapter)

MongoDB Adapter for [ProophEventStore](https://github.com/prooph/event-store)

Requirements:
-------------

- Mongo DB >= 2.6.0
- PHP Mongo Extension >= 1.5.0

Write concern:
--------------

This adapter uses a transaction timeout of 50 secs by default.

The default write concern for this adapater is acknowledged and journaled (['w' => 1, 'j' => true]).

It's possible to change both values by injecting them into the constructor or by using the factory.
