# Change Log

## [v2.4.1](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.4.1) (2017-02-21)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.4.0...v2.4.1)

**Implemented enhancements:**

- Add compound index for transaction\_id and expire\_at [\#61](https://github.com/prooph/event-store-mongodb-adapter/pull/61) ([sandrokeil](https://github.com/sandrokeil))

**Closed issues:**

- Do we need an index on transaction\_id ? [\#60](https://github.com/prooph/event-store-mongodb-adapter/issues/60)
- use native MongoDB data types whenever possible [\#53](https://github.com/prooph/event-store-mongodb-adapter/issues/53)

It is necessary to add this index manually to existing collection streams. Run this in your mongo shell:

db.[your stream collection].createIndex({"transaction_id" : 1, "expire_at" : 1})

## [v2.4.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.4.0) (2016-07-22)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.3.0...v2.4.0)

**Fixed bugs:**

- Create missing index for replaying [\#59](https://github.com/prooph/event-store-mongodb-adapter/pull/59) ([prolic](https://github.com/prolic))

## [v2.3.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.3.0) (2016-07-04)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.2.0...v2.3.0)

**Implemented enhancements:**

- allow php7 [\#57](https://github.com/prooph/event-store-mongodb-adapter/pull/57) ([prolic](https://github.com/prolic))

## [v2.2.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.2.0) (2016-06-28)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.1.1...v2.2.0)

**Implemented enhancements:**

- Concurrency exception [\#56](https://github.com/prooph/event-store-mongodb-adapter/pull/56) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- better concurrency handling [\#58](https://github.com/prooph/event-store-mongodb-adapter/pull/58) ([prolic](https://github.com/prolic))

## [v2.1.1](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.1.1) (2016-05-14)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.1.0...v2.1.1)

## [v2.1.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.1.0) (2016-05-08)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.0.3...v2.1.0)

**Merged pull requests:**

- Prepare 2.1.0 Release [\#55](https://github.com/prooph/event-store-mongodb-adapter/pull/55) ([codeliner](https://github.com/codeliner))
- Readme typo [\#54](https://github.com/prooph/event-store-mongodb-adapter/pull/54) ([jpkleemans](https://github.com/jpkleemans))

## [v2.0.3](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.0.3) (2016-04-01)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.0.2...v2.0.3)

**Fixed bugs:**

- Unique index for aggregate\_id + version required to handle concurrent writes [\#49](https://github.com/prooph/event-store-mongodb-adapter/issues/49)

**Merged pull requests:**

- update factories to interop-config 1.0 [\#52](https://github.com/prooph/event-store-mongodb-adapter/pull/52) ([sandrokeil](https://github.com/sandrokeil))

## [v2.0.2](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.0.2) (2016-03-04)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.0.1...v2.0.2)

**Implemented enhancements:**

- Update to coveralls ^1.0 [\#51](https://github.com/prooph/event-store-mongodb-adapter/pull/51) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- add unique index for aggregate\_id + version [\#50](https://github.com/prooph/event-store-mongodb-adapter/pull/50) ([prolic](https://github.com/prolic))

**Closed issues:**

- Update to coveralls ^1.0 [\#47](https://github.com/prooph/event-store-mongodb-adapter/issues/47)

## [v2.0.1](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.0.1) (2015-12-08)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.0...v2.0.1)

**Fixed bugs:**

- fix exception message typo [\#46](https://github.com/prooph/event-store-mongodb-adapter/pull/46) ([prolic](https://github.com/prolic))

## [v2.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.0) (2015-11-22)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v2.0-beta.1...v2.0)

**Fixed bugs:**

- it can commit and rollback empty transactions [\#40](https://github.com/prooph/event-store-mongodb-adapter/pull/40) ([prolic](https://github.com/prolic))

**Merged pull requests:**

- v2.0 [\#45](https://github.com/prooph/event-store-mongodb-adapter/pull/45) ([codeliner](https://github.com/codeliner))
- fix typo [\#44](https://github.com/prooph/event-store-mongodb-adapter/pull/44) ([prolic](https://github.com/prolic))
- remove php7 support for now [\#43](https://github.com/prooph/event-store-mongodb-adapter/pull/43) ([prolic](https://github.com/prolic))
- Test replay of two aggregates [\#39](https://github.com/prooph/event-store-mongodb-adapter/pull/39) ([codeliner](https://github.com/codeliner))

## [v2.0-beta.1](https://github.com/prooph/event-store-mongodb-adapter/tree/v2.0-beta.1) (2015-10-21)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v1.0...v2.0-beta.1)

**Implemented enhancements:**

- Make use of interop config [\#32](https://github.com/prooph/event-store-mongodb-adapter/issues/32)
- Make use of interop config [\#37](https://github.com/prooph/event-store-mongodb-adapter/pull/37) ([prolic](https://github.com/prolic))
- fix namespace organisation in tests [\#35](https://github.com/prooph/event-store-mongodb-adapter/pull/35) ([prolic](https://github.com/prolic))
- remove branch alias [\#31](https://github.com/prooph/event-store-mongodb-adapter/pull/31) ([prolic](https://github.com/prolic))
- add replay functionality [\#30](https://github.com/prooph/event-store-mongodb-adapter/pull/30) ([prolic](https://github.com/prolic))
- implement event stream as an iterator [\#29](https://github.com/prooph/event-store-mongodb-adapter/pull/29) ([prolic](https://github.com/prolic))
- Fix bad transaction support [\#27](https://github.com/prooph/event-store-mongodb-adapter/pull/27) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- Bad transaction support [\#26](https://github.com/prooph/event-store-mongodb-adapter/issues/26)
- fix exception message [\#36](https://github.com/prooph/event-store-mongodb-adapter/pull/36) ([prolic](https://github.com/prolic))
- replay orders by created\_at first [\#34](https://github.com/prooph/event-store-mongodb-adapter/pull/34) ([prolic](https://github.com/prolic))
- Fix metadata handling of iterator [\#33](https://github.com/prooph/event-store-mongodb-adapter/pull/33) ([codeliner](https://github.com/codeliner))
- Fix bad transaction support [\#27](https://github.com/prooph/event-store-mongodb-adapter/pull/27) ([prolic](https://github.com/prolic))

**Merged pull requests:**

- event store 6.0-beta dep [\#38](https://github.com/prooph/event-store-mongodb-adapter/pull/38) ([codeliner](https://github.com/codeliner))
- Return insertBatch without assigning it to non existing property [\#28](https://github.com/prooph/event-store-mongodb-adapter/pull/28) ([codeliner](https://github.com/codeliner))

## [v1.0](https://github.com/prooph/event-store-mongodb-adapter/tree/v1.0) (2015-08-28)
[Full Changelog](https://github.com/prooph/event-store-mongodb-adapter/compare/v0.1...v1.0)

**Implemented enhancements:**

- Make use of MongoWriteBatch class [\#10](https://github.com/prooph/event-store-mongodb-adapter/issues/10)
- Remove unwanted exception [\#24](https://github.com/prooph/event-store-mongodb-adapter/pull/24) ([prolic](https://github.com/prolic))
- Fix datetime problem [\#23](https://github.com/prooph/event-store-mongodb-adapter/pull/23) ([prolic](https://github.com/prolic))
- Adjust adapter to support prooph/event-store 5.0 [\#21](https://github.com/prooph/event-store-mongodb-adapter/pull/21) ([prolic](https://github.com/prolic))
- Use MongoWriteBatch, WriteConcern & Container namespace [\#17](https://github.com/prooph/event-store-mongodb-adapter/pull/17) ([prolic](https://github.com/prolic))

**Fixed bugs:**

- Fix datetime problem [\#23](https://github.com/prooph/event-store-mongodb-adapter/pull/23) ([prolic](https://github.com/prolic))

**Closed issues:**

- Support datetime with microseconds [\#22](https://github.com/prooph/event-store-mongodb-adapter/issues/22)
- Add Write Concern Levels [\#12](https://github.com/prooph/event-store-mongodb-adapter/issues/12)
- Adjust adapter to support prooph/event-store 5.0 [\#11](https://github.com/prooph/event-store-mongodb-adapter/issues/11)
- remove \[wip\] from project description on github [\#9](https://github.com/prooph/event-store-mongodb-adapter/issues/9)
- Feature: Store events in different event collections based on the stream\_name [\#7](https://github.com/prooph/event-store-mongodb-adapter/issues/7)
- The backend should treat all Dates as UTC [\#6](https://github.com/prooph/event-store-mongodb-adapter/issues/6)
- Question: Do we need support for nested transaction? [\#5](https://github.com/prooph/event-store-mongodb-adapter/issues/5)
- How to handle maximum document size? [\#3](https://github.com/prooph/event-store-mongodb-adapter/issues/3)
- How to handle transactions [\#2](https://github.com/prooph/event-store-mongodb-adapter/issues/2)
- Native driver or doctrine abstraction [\#1](https://github.com/prooph/event-store-mongodb-adapter/issues/1)

**Merged pull requests:**

- test php7 on travis [\#25](https://github.com/prooph/event-store-mongodb-adapter/pull/25) ([prolic](https://github.com/prolic))
- Document mongo sharding [\#20](https://github.com/prooph/event-store-mongodb-adapter/pull/20) ([prolic](https://github.com/prolic))
- Add copyright information to all php files [\#19](https://github.com/prooph/event-store-mongodb-adapter/pull/19) ([prolic](https://github.com/prolic))
- Fix typo [\#18](https://github.com/prooph/event-store-mongodb-adapter/pull/18) ([prolic](https://github.com/prolic))
- Change constructor signature and provide a factory [\#16](https://github.com/prooph/event-store-mongodb-adapter/pull/16) ([prolic](https://github.com/prolic))
- Cleanup .php\_cs config file [\#15](https://github.com/prooph/event-store-mongodb-adapter/pull/15) ([prolic](https://github.com/prolic))
- Use short array notation [\#14](https://github.com/prooph/event-store-mongodb-adapter/pull/14) ([prolic](https://github.com/prolic))
- Add php-cs-fixer [\#13](https://github.com/prooph/event-store-mongodb-adapter/pull/13) ([prolic](https://github.com/prolic))
- Improve phpdoc [\#8](https://github.com/prooph/event-store-mongodb-adapter/pull/8) ([prolic](https://github.com/prolic))

## [v0.1](https://github.com/prooph/event-store-mongodb-adapter/tree/v0.1) (2015-08-09)
**Merged pull requests:**

- initial mongo db adapter [\#4](https://github.com/prooph/event-store-mongodb-adapter/pull/4) ([prolic](https://github.com/prolic))



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
