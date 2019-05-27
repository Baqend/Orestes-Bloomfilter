## 2.2.2

* Apply secure config values to existing bloom filter

## 2.2.1

* Java 8 compatibility
* Some minor changes in the build process

## 2.2.0

* Implement soft clear for expiring Bloom filter to clear the filter without deleting any TTLs

## 2.1.0

* Implement a grace period which keeps expired entries in the TTL map for a configurable addtional time period
* Allow checking whether an item is expired, but still within the grace period by ExpiringBloomFilter#isKnown
* Fix a bug that prevented expired items from being removed from the expiration TTL map

## 2.0.0

* **BREAKING CHANGES:** A critical error with the hash entry calculation for counting Bloom filters has been fixed.
  Hashes were encoded incorrectly by Jedis in the 1.x-based releases.
  Therefore, the Redis-backed counting Bloom filter implementation is not backward compatible to older releases.
  To avoid problems during upgrade, make sure to start with a clean Redis when upgrading to 2.0!
* Introduced a new expiring Bloom filter which maintains Bloom filter entry expiration completely in Redis 
* Counting and expiration-based Bloom filters can be migrated between in-memory and Redis-backed implementations
* Many small improvements and a general interface overhaul

## 1.2.3

* Better implementation of ExpiringBloomFilter#reportWrites bulk methods

## 1.2.1

* Add interface for ExpiringBloomFilter#reportWrites bulk methods

## 1.2.0

* Make the RedisPool configurable #42
* Support sentinel and master/slave setups for the redis backed Bloom Filter #39
* Support database number to support redis namespaces 
* Fix NullPointerException in CountingBloomFilterRedis #40 

## 1.1.9

* Implement SSL configuration for redis #38

## 1.1.8

* Implement ExpiringBloomFilterMemory#clear()

## 1.1.7

* Changed default hash function to Murmur3KirschMitzenmacher

## 1.1.6

* Fixed and tested problem in ExpirationQueue

## 1.1.5

* Fixed and tested problem in ExpirationQueue

## 1.1.4

* Fixed a problem with overwritten Redis paramters in existing Bloom filters

## 1.1.3

* Fix overwritten host names when the config builder was copied

## 1.1.2

* Fixed overflow and underflow behaviour in the memory Counting Bloom filters
* Added test for reusing RedisPools

## 1.1.1

* Fixed Overflow Behaviour in the memory Counting Bloom filters
* Added possibility to add Redis Pool in the Builder

## 1.1.0

* Added authentication
* Fixed Race Condition in getBitSet
* Fixed Problem with byte[] signatures and Generics

## 1.0.8

* Some improvements on the expiring Bloom Filter

## 1.0.7

* Several bug fixes
* improved tests
* Added the expiring Bloom filter

## 1.0.6

* Fixed bug in JSON Converter and added conversion testing

## 1.0.5

* performance optimizations for 8, 16, 32, 64 bit in-memory counting Bloom filters through native arrays (speed improvement ~ 10x)

## 1.0.4

* Fixed Jedis-related Stackoverflow issue in bulk methods
