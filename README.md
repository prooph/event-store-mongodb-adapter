# event-store-mongodb-adapter

[![Build Status](https://travis-ci.org/prooph/event-store-mongodb-adapter.svg?branch=master)](https://travis-ci.org/prooph/event-store-mongodb-adapter)
[![Coverage Status](https://coveralls.io/repos/prooph/event-store-mongodb-adapter/badge.svg?branch=master&service=github)](https://coveralls.io/github/prooph/event-store-mongodb-adapter?branch=master)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/prooph/improoph)

MongoDB Adapter for [ProophEventStore](https://github.com/prooph/event-store)

#CAUTION: The mongoDB adapter is **NOT** compatible With prooph/event-store v7. MongoDB has limited ACID support which is not compatible with the new features of prooph/event-store. MongoDB is a great choice for a read model database, but unfortunately it cannot be used as an event-store that requires transactions across documents (events). 
Codeliner has published a [Gist](https://gist.github.com/codeliner/14a8d98d53efafdd35e851d76e89cc94) that shows a custom implementation of a possible mongoDB event store supporting a subset of the v7 features in a very limited way. If you want to try to bring mongoDB back, please get in touch with us and we can discuss it! 

**Support for the adapter will end at 31 December 2017.**

Requirements
------------

- MongoDB >= 4.0
- MongoDB PHP Driver >= 1.5.2

Transactions
-------------

The transaction write concern for this adapter is `majority`.

The transaction read concern for this adapter is `snapshot`.

You can disable transactions for this adapter.

Considerations
--------------

This adapter does not use the MongoDB ObjectId for its primary key, instead a UUID (string) is used.

We recommend the AggregateStreamStrategy as the best strategy to use with this adapter.

Keep in mind that transaction safety works only for a replica set. Sharded cluster support is planned for MongoDB 4.2.
Therefore it's not safe to use this adapter in a sharded cluster environment, as MongoDB can't guarantee transaction safety.

Stream can not be reset if iteration was started due the MongoDB cursor.