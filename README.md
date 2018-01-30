Bloom filter library
================================

[Changelog](CHANGELOG.md) | [Setup](#install) | [Docs](#usage) | [Maven Repo](https://bintray.com/baqend/maven/Orestes-Bloomfilter)

Version 1 is out with a complete rewrite of almost all functionalities and many new ones.

This is a set of Bloom filters we implemented as we found all existing open-source implementations to be lacking in various aspects. This library takes some inspiration from the [simple Bloom filter implementation of Magnus Skjegstad](https://github.com/MagnusS/Java-BloomFilter) and the [Ruby Bloom filters by Ilya Grigorik](https://github.com/igrigorik/bloomfilter-rb).

The Bloom filter is a probabilistic set data structure which is very small. This is achieved by allowing false positives with some probability *p*. It has an `add` and `contains` operation which both are very fast (time complexity *O(1)*). The Counting Bloom filter is an extension of the Bloom filter with a `remove` operation at the cost of incurring an additional space overhead for counting. There are many good introductions to Bloom filters: the [Wikipedia article](http://en.wikipedia.org/wiki/Bloom_filter) is excellent, and even better is a [survey by Broder and Mitzenmacher](http://www.cs.utexas.edu/~yzhang/teaching/cs386m-f8/Readings/im2005b.pdf). Typical use cases of Bloom filters are content summaries and sets that would usually grow too large in fields such as networking, distributed systems, databases and analytics.

There are 4 types of Bloom filters in the Orestes Bloom filter library:
* **Regular Bloom filter**, a regular in-memory Java Bloom filter (`MemoryBloomFilter`)
* **Counting Bloom filter**, a Counting Bloom Filter which supports element removal (`MemoryCountingBloomFilter`)
* **Redis Bloom Filter**, a Redis-backed Bloom filter which can be concurrently used by different applications (`RedisBloomFilter`)
* **Redis Counting Bloom Filter**, a Redis-backed Bloom filter which can be concurrently used by different applications, it keeps track of the number of keys added to the filter (`RedisCountingBloomFilter`)

This library if written in and for Java 8. For a Java 7 backport see: https://github.com/Crystark/Orestes-Bloomfilter

## Err, Bloom what?
Bloom filters are awesome data structures: **fast *and* space efficient**.
```java
BloomFilter<String> bf = new FilterBuilder(10_000_000, 0.01).buildBloomFilter(); //Expect 10M URLs
urls.add("http://github.com"); //Add millions of URLs
urls.contains("http://twitter.com"); //Know in an instant which ones you have or have not seen before
```
So what's the catch? Bloom filters allow false positives (i.e. URL contained though never added) with some  probability (0.01 in the example). If you can mitigate rare false positives (false negatives never happen) then Bloom filters are probably for you.

## New in 2.0
* A new expiring Bloom filter which maintains Bloom filter entry expiration completely in Redis
  (ExpiringBloomFilterPureRedis.java)
* Counting and expiration-based Bloom filters can now be migrated between the in-memory and Redis-backed implementations
* fixed a critical error with the hash entry calculation for counting Bloom filters:
  Hashes were encoded incorrectly by Jedis in the 1.x-based releases.
  Therefore, the Redis-backed counting Bloom filter implementation is not backward compatible with older releases.
  To avoid problems during upgrade, make sure to start with a clean Redis when upgrading to 2.0!

## New in 1.0
* Bloom filters are now constructed and configured using a comfortable Builder interface, e.g. `new FilterBuilder(100,0.01).redisBacked().buildCountingBloomFilter()`
* All Bloom filters are thread-safe and drastically improved in performance
* Arbitrarily many Bloom filter instances in a single Redis server by using `name("myfilter")` to distinguish filters
* Existing filter can be loaded or overwritten and be shared from different processes without concurrency anomalies
* New and improved Hash functions: All cryptographic hash functions, Murmur3 and Murmur3 with the <a href="https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf">Kirsch&Mitzenmacher trick</a>, advanced distribution and performance testing of hash functions
* Redis Bloom filters are much faster and simpler, fixed a very rare race-condition
* Memory and Redis Bloom filters now share a common interface `BloomFilter` resp. `CountingBloomFilter` instead of being subclasses
* Extensive JavaDoc documentation, test-coverage increased by a factor of at least 2, cleaner a streamlined (Java 8) design
* Redis read-slaves: allow your Bloom filter to perform reads on slaves to get even higher performance
* Library now available as Maven/Gradle repo and built using Gradle
* Population Estimation: the population of Counting and normal Bloom Filter can now be <a href="http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter">precisely estimated</a>
* Frequency Estimation: the frequency/count of elements in Counting Bloom filter can now be estimated using the Minimum-Selection algorithm (known from <a href="http://theory.stanford.edu/~matias/papers/sbf_thesis.pdf">spectral Bloom filters</a>
* All add and remove method variants now return whether the element was added/removed resp. what the element's estimated count is
* Redis Bloom filters now use configurable connection pooling and are thus not limited by round-trip times anymore
* The library is now an important component of our Backend-as-a-Service startup <a href="https://www.baqend.com">Baqend</a> and thus you can expect far more frequent updates. Don't worry, the Bloom filter library will always remain MIT-licensed and open-source!


## Features
There are a many things we addressed as we sorely missed them in other implementations:
* Bloom filter and Counting Bloom filter in both a local and shared variants with the same interface
* Configuration of all parameters: Bit-Array size *m*, number of hash functions *k*, counting bits *c*
* Automatic configuration given the tolerable false positive rate *p* and expected elements *n*
* Statistics, e.g. what is my current false positive probability?
* Choice among different hash functions: the better (i.e. uniformly distributed) the hash function, the more accurate the Bloom filter but the better the hash function usually the slower it is -> choose from about 10-15  optimized hash functions, e.g. MD5, SHA, Murmur, LCGs, Carter-Wegman etc. or use a custom one
* Operations on the shared Bloom filter need to be fast (single round-trips to Redis per operation and heavy use of pipelining)
* Generation of the Bloom filter is always fast (on-the-fly pregeneration)
* Support of union and intersection
* Implementation of [rejection sampling](http://en.wikipedia.org/wiki/Rejection_sampling) and chaining of hash values taking into account the [avalanche effect](http://en.wikipedia.org/wiki/Avalanche_effect) (higher hash quality)
* Minimal dependencies: the local Bloom filters have none, the Redis Bloom filters need the [jedis](https://github.com/xetorthio/jedis) client library (in  `lib` folder)
* Concurrency: the shared Bloom filter can be accessed by many clients simultaneously without multi-user anomalies and performance degradation (which is quite difficult for bitwise counters and a pregnerated Bloom filter - but possible)

<a name="install"/>
## Getting started
*New*: The Bloom filter repository is now hosted on [JCenter](https://bintray.com/baqend/maven/Orestes-Bloomfilter/view).

Java 8 is required. The recommended way to include the Bloom filter is via the Maven repo (works for Gradle, Ivy, etc., 
too):

```xml
<dependencies>
   <dependency>
       <groupId>com.baqend</groupId>
       <artifactId>bloom-filter</artifactId>
       <version>1.0.7</version>
   </dependency>
</dependencies>
<repositories>
   <repository>
       <snapshots>
        <enabled>false</enabled>
       </snapshots>
       <id>central</id>
       <name>bintray</name>
       <url>http://jcenter.bintray.com</url>
   </repository>
</repositories>
```

or with Gradle:

```groovy
repositories {
    jcenter()
}
dependencies {
    compile(
            'com.baqend:bloom-filter:1.0.7'
    )
}
```


For the normal Bloom filters it's even sufficient to only copy the source *.java files to your project (not recommended).

<a name="usage"/>
## Usage
- [Regular Bloom Filter](#a1)
- [The Filter Builder](#builder)
- [Counting Bloom Filter](#a2)
- [Redis Bloom Filters](#a3)
- [Redis Counting Bloom Filters](#a4)
- [Read Slaves](#slaves)
- [JSON Representation](#a5)
- [Hash Functions](#a6)
- [Performance](#a7)
- [Overview of Probabilistic Data Structures](#overview)

<a name="a1"/>
### Regular Bloom Filter
The regular Bloom filter is very easy to use. It is the base class of all other Bloom filters. Figure out how many elements you expect to have in the Bloom filter ( *n* ) and then which false positive rate is tolerable ( *p* ).

```java
//Create a Bloom filter that has a false positive rate of 0.1 when containing 1000 elements
BloomFilter<String> bf = new FilterBuilder(1000, 0.1).buildBloomFilter();
```

The Bloom filter class is generic and will work with any type that implements the `toString()` method in a sensible way, since that String is what the Bloom filter feeds into its hash functions. The `hashCode()` method is not used, since in Java it returns integers that normally do not satisfy a uniform distribution of outputs that is essential for the optimal performance of the Bloom filter. Now lets add something:

```java
//Add a few elements
bf.add("Just");
bf.add("a");
bf.add("test.");
```

This can be done from different threads - all Bloom filters are now thread-safe. An element which was inserted in a Bloom filter will always be returned as being contained (no false negatives):

```java
//Test if they are contained
print(bf.contains("Just")); //true
print(bf.contains("a")); //true
print(bf.contains("test.")); //true
```

Usually non-inserted elements will not be contained:
```java
//Test with a non-existing element
print(bf.contains("WingDangDoodel")); //false
```
If we add enough elements, false positives will start occurring:
```java
//Add 300 elements
for (int i = 0; i < 300; i++) {
	String element = "Element " + i;
	bf.add(element);
}	
//test for false positives
for (int i = 300; i < 1000; i++) {
	String element = "Element " + i;
	if(bf.contains(element)) {
		print(element); //two elements: 440, 669
	}
}
```
Let's compare this with the expected amount of false positives:
```java
//Compare with the expected amount
print(bf.getFalsePositiveProbability(303) * 700); //1.74
```
So our two false positives are in line with the expected amount of 1.74.

and lets "estimate" how many elements are in the filter using statistically sound computations of the amount of bits that are one:
```java
//Estimate cardinality/population
print(bf.getEstimatedPopulation()); //303.6759147801151
```
This estimation is very good, even though the estimation was performed on a "quite full" Bloom filter (remember, we allowed the false positive probability to be 10% for 1000 elements).

The Bloom filter can be cleared and cloned:
```java
//Clone the Bloom filter
bf.clone();
//Reset it, i.e. delete all elements
bf.clear();
```

Also elements can be added and queried in bulk:
```java
List<String> bulk = Arrays.asList(new String[] { "one", "two", "three" });
bf.addAll(bulk);
print(bf.containsAll(bulk)); //true
```

To get the best performance for a given use-case the parameters of the bloom filter must be chosen wisely. So for example we could choose the Bloom filter to use 1000 Bits and then use the best number of hash functions for an expected amount of 6666 inserted elements. We choose Murmur as our hash function which is faster than cryptographic hash functions like MD5:

```java
//Create a more customized Bloom filter
BloomFilter<Integer> bf2 = new FilterBuilder()
                .expectedElements(6666) //elements
                .size(10000) //bits to use
                .hashFunction(HashMethod.Murmur3) //our hash
                .buildBloomFilter();
print("#Hashes:" + bf2.getHashes()); //2
print(FilterBuilder.optimalK(6666, 10000)); //you can also do these calculations yourself
```

Bloom filters allow other cool stuff too. Consider for instance that you collected two Bloom filters which are compatible in their parameters. Now you want to consolidate their elements. This is achieved by ORing the respective Bit-Arrays of the Bloom filters:
```java
//Create two Bloom filters with equal parameters
BloomFilter<String> one = new FilterBuilder(100, 0.1).buildBloomFilter();
BloomFilter<String> other = new FilterBuilder(100, 0.1).buildBloomFilter();
one.add("this");
other.add("that");
one.union(other);
print(one.contains("this")); //true
print(one.contains("that")); //true
```

The good thing about the `union()` operation is, that it returns the exact Bloom filter which would have been created, if all elements were inserted in one Bloom filter from the get-go. There is a similar `intersect` operation that ANDs the Bit-Arrays. It does however behave slightly different as it does not return the Bloom filter that only contains the 
intersection. It guarantees to have all elements of the intersection but the false positive rate might be slightly higher than that of the pure intersection:

```java
other.add("this");
other.add("boggles");
one.intersect(other);
print(one.contains("this")); //true
print(one.contains("boggles")); //false
```

<a name="builder"/>
### The Filter Builder
The `FilterBuilder` is used to configure Bloom filters before constructing them. It will try to infer and compute any missing parameters optimally and preconfigured with sensible defaults (documented in its JavaDoc). For instance if you only specified the number of expected elements and the false positive probability, it will compute the optimal bit size and number of hash functions.
To construct a filter, you can either call `buildBloomFilter` or `buildCountingBloomFilter` or you can pass the builder to a specific Bloom filter implementation to construct it.

<a name="a2"/>
## Counting Bloom Filter
The Counting Bloom filter allows object removal. For this purpose it has binary counters instead of simple bits. The 
amount of bits *c* per counter can be set. If you expect to insert elements only once, the 
probability of a Bit overflow is very small for *c = 4* : *1.37 * 10^-15 * m* for up to *n* inserted elements  ([details](http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html#SECTION00053000000000000000)). For those 
use-cases 4 bits are usually the most space-efficient choice. The default however is 16 bits, so you don't have to 
worry about counter overflow with the downside of some space overhead. Keep in mind that if you insert items more 
than once you need larger counters, i.e. roughly *Log(maximum_inserts)/Log(2) + 4* bits. And it is in fact useful to 
insert non-unique items since then you can perform frequency estimation ("how often have I seen this item?") using 
`addAndEstimateCount` and `getEstimatedCount`.

```java
//Create a Counting Bloom filter that has a FP rate of 0.01 when 1000 are inserted
//and uses 4 bits for counting
CountingBloomFilter<String> cbf = new FilterBuilder(1000, 0.01).buildCountingBloomFilter();
cbf.add("http://google.com");
cbf.add("http://twitter.com");
print(cbf.contains("http://google.com")); //true
print(cbf.contains("http://twitter.com")); //true
```

If you insert one distinct item multiple times, the same counter always get updated so you should pick a higher *c* 
so that *2^c > inserted_copies*. When 8, 16, 32, 64 bits are specified as the counter size, internally an optimized 
short-, byte-, int- resp. long-array will be used, whereas other sizes will use a custom bit vector build on the <a 
href="http://docs.oracle.com/javase/8/docs/api/java/util/BitSet.html">Java BitSet</a>. For optimal performance in 
terms of time complexity you should therefore prefer 8, 16, 32, 64 counting bits.

The Counting Bloom Filter extends the normal Bloom Filter by `remove` and `removeAll` methods:

```java
//What only the Counting Bloom filter can do:
cbf.remove("http://google.com");
print(cbf.contains("http://google.com")); //false
```

To handle overflows (which is unlikely to ever be an issue) you can set an overflow callback:

```java
//Use the Memory Bloom filter explicitly (for the overflow method):
FilterBuilder fb = new FilterBuilder(1000, 0.01).countingBits(4);
CountingBloomFilterMemory<String> filter = new CountingBloomFilterMemory<>(fb);
filter.setOverflowHandler(() -> print("ups"));
for (int i = 1; i < 20; i++) {
        print("Round " + i);
        filter.add("http://example.com"); //Causes onOverflow() in Round >= 16
}
```

To understand the inner workings of the Counting Bloom filter lets actually look at the bits of a small filter:

```java
CountingBloomFilter<String> small = new FilterBuilder(3, 0.2)
                .countingBits(4)
                .buildCountingBloomFilter();
small.add("One"); small.add("Two"); small.add("Three");
print(small.toString());
```
This gives:
```bash
Bloom Filter Parameters: size = 11, hashes = 3, Bits: {0, 2, 6, 8, 10}
1 0001
0 0000
1 0001
0 0000
0 0000
0 0000
1 0001
0 0000
1 0001
0 0000
1 0101
```

The Counting Bloom filter thus has a bit size of 11, uses 3 hash functions and 4 bits for counting. The first row is the materialized bit array of all counters > 0. Explicitly saving it makes `contains` calls fast and generation when transferring the Counting Bloom Filter flattened to a Bloom filter.


<a name="a3"/>
## Redis Bloom Filters
Bloom filters are really interesting as they allow very high throughput and minimal latency for adding and querying (and removing). Therefore you might want to use them across the boundaries of a single machine. For instance imagine you run a large scale web site or web service. You have a load balancer distributing the request load over several front-end web servers. You now want to store some information with a natural set structure, say, you want to know if a source IP address has accessed the requested URL in the past. You could achieve that by either explicitly storing that information (probably in a database) which will soon be a bottleneck if you serve billions of requests a day. Or you employ a shared Bloom filter and accept a small possibility of false positives.

These kind of use-cases are ideal for the Redis-backed Bloom filters of this library. They have the same Java Interfaces as the normal and Counting Bloom filter but store the Bloom filter bits in the [in-memory key-value store Redis](http://redis.io).

Reasons to use these Redis-backed Bloom filters instead of their pure Java brothers are:
* **Distributed** Access to on Bloom filter
* **Persistence** Requirements (e.g. saving the Bloom filter to disk once every second)
* **Scalability** of the Bloom Filter beyond one machine using replication to speed up all read operations

Using the Redis-backed Bloom filter is straightforward:

1. Install Redis. This is extremely easy: [see Redis Installation](http://redis.io/download).
2. Start Redis with `$ redis-server`. The server will listen on port 6379.
3. In your application (might be on a different machine) instantiate a Redis-backed Bloom filter giving the IP or host name of Redis and its port and the number of concurrent connections to the FilterBuilder using `redisHost`, `redisPort`, `redisConnections`.

The Redis-backed Bloom filters have the same interface as the normal Bloom filters and can be constructed through the FilterBuilder, too:

```java
String host = "localhost";
int port = 6379;
String filterName = "normalbloomfilter";
//Open a Redis-backed Bloom filter
BloomFilter<String> bfr = new FilterBuilder(1000, 0.01)
    .name(filterName) //use a distinct name
    .redisBacked(true)
    .redisHost(host) //Default is localhost
    .redisPort(port) //Default is standard 6379
    .buildBloomFilter();
    
bfr.add("cow");

//Open the same Bloom filter from anywhere else
BloomFilter<String> bfr2 = new FilterBuilder(1000, 0.01)
    .name(filterName) //load the same filter
    .redisBacked(true)
    .buildBloomFilter();
bfr2.add("bison");
        
print(bfr.contains("cow")); //true
print(bfr.contains("bison")); //true
```

The Redis-backed Bloom filters are concurrency/thread-safe at the backend as-well-as in Java. That means you can concurrently insert from any machine without running into anomalies, inconsistencies or lost data. The Redis-backed Bloom filters are implemented using efficient [Redis bit arrays](http://redis.io/commands/getbit). They make heavy use of [pipelining](http://redis.io/topics/pipelining) so that every `add` and `contains` call only needs one round-trip. This is the most performance critical aspect and usually not found in [other implementations](https://github.com/igrigorik/bloomfilter-rb) which need one round-trip for every Bit or worse. Moreover, Redis connections are pooled so they are reused, while profiting from concurrent use.

The Redis-backed Bloom filters save their metadata (like number and kind of hash functions) in Redis, too. Thus other clients can easily to connect to a Redis instance that already holds a Bloom filter with a given name and specify whether to use or overwrite it.

<a name="a4"/>
## Redis Counting Bloom Filters
The Redis Counting Bloom filter saves the counters as separate counters in a compact [Redis hash](http://redis
.io/commands#hash) and keeps the materialized flat Bloom filter as a bit array. It is compatatible with Redis 2.4 or 
higher.

```java
//Open a Redis-backed Counting Bloom filter
CountingBloomFilter<String> cbfr = new FilterBuilder(10000, 0.01)
                .name("myfilter")
                .overwriteIfExists(true) //instead of loading it, overwrite it if it's already there
                .redisBacked(true)
                .buildCountingBloomFilter();
        cbfr.add("cow");

        //Open a second Redis-backed Bloom filter with a new connection
        CountingBloomFilter<String> bfr2 = new FilterBuilder(10000, 0.01)
                .name("myfilter") //this time it will be load it
                .redisBacked(true)
                .buildCountingBloomFilter();
        bfr2.add("bison");
        bfr2.remove("cow");

        print(cbfr.contains("bison")); //true
        print(cbfr.contains("cow")); //false
```

<a name="slaves"/>
## Redis Bloom Filter Read Slaves
If your workloads on the Bloom filter are *really* high-throughput you can leverage read-slaves. They will be queried for any reading operations: contains, fetching of the bit set, estimation methods (population, count, etc.):

```java
CountingBloomFilter<String> filter = new FilterBuilder(m,k)
                .name("slavetest")
                .redisBacked(true)
                .addReadSlave(host, port +1); //add slave
                .addReadSlave(host, port +2); //and another
                .buildCountingBloomFilter() //or a normal one
                
filter.containsAll(items); //directed to the slave
filter.getEstimatedPopulation(); //that one too
filter.getEstimatedCount("abc"); //dito
filter.getBitSet(); //and again
```

<a mame="sentinel"/>
## Redis Sentinel Bloom Filters
To configure a Bloom Filter to use Sentinel to find the master Redis node, when building the FilterBuilder explicitly define a Sentinel configuration and provide your own Pool.

In the following example the Sentinel Nodes are a simple Set of form "host:port" and the sentinelClusterName is the name of Sentinel you want to connect to
```java
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .pool(RedisPool.sentinelBuilder()
                      .master(sentinelClusterName)
                      .sentinels(getSentinelNodes())
                      .database(database)
                      .redisConnections(connections)
                      .build())
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
```

<a name="a5"/>
## JSON Representation
To easily transfer a Bloom filter to a client (for instance via an HTTP GET) there is a JSON Converter for the Bloom filters. All Bloom filters are implemented so that this generation option is very cheap (i.e. just sequentially reading it from memory). It works for all Bloom filters including the ones backed by Redis.
```java
BloomFilter<String> bf = new FilterBuilder().expectedElements(50).falsePositiveProbability(0.1).buildBloomFilter();
bf.add("Ululu");
JsonElement json = BloomFilterConverter.toJson(bf);
print(json); //{"size":240,"hashes":4,"HashMethod":"MD5","bits":"AAAAEAAAAACAgAAAAAAAAAAAAAAQ"}
BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
print(otherBf.contains("Ululu")); //true
```
JSON is not an ideal format for binary content (Base64 only uses 64 out of 94 possible characters) but it's highly interoperable and easy to read which outweighs the slight waste of space. Combining it with a [Content-Encoding](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html) like gzip usually compensates the overhead.

Moreover, the Memory Counting Bloom filter can also be serialized and deserialized in the normal Java way.

<a name="a6"/>
## Hash Functions
There is a detailed description of the available hash functions in the Javadocs of the HashMethod enum. Hash uniformity (i.e. all bits of the Bloom filter being equally likely) is of great importance for the false positive rate. But there is also an inherent trade-off between hash uniformity and speed of computation. For instance cryptographic hash functions have very good distribution properties but are very CPU intensive. Pseudorandom number generators like the [linear congruential generator](http://en.wikipedia.org/wiki/Linear_congruential_generator) are easy to compute but do not have perfectly random outputs but rather certain distribution patterns which for some inputs are notable and for others are negligible. The implementations of all hash functions are part of the BloomFilter class and use tricks like [rejection sampling](https://en.wikipedia.org/wiki/Rejection_sampling) to get the best possible distribution for the respective hash function type.

Here is a Box plot overview of how good the different hash functions perform (Intel i7 with 4 cores, 16 GB RAM). The configuration is 100000 hashes using k = 10, m = 1000 averaged over 20 runs. 

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/hash-speed.png"/> 

Speed of computation doesn't tell anything about the quality of hash values. A good hash function is one, which has a discrete uniform distribution of outputs. That means that every bit of the Bloom filter's bit vector is equally likely to bet set. To measure if and how good the hash functions follow a uniform distribution [goodness of fit Chi-Square hypothesis tests](http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test) are the mathematical instrument of choice.

Here are some of the results. The inputs are random strings. The p-value is the probability of getting a statistical result that is at least as extreme as the obtained result. So the usual way of hypothesis testing would be rejecting the null hypothesis ("the hash hash function is uniformly distributed") if the p-value is smaller than 0.05. We did 100 Chi-Square Tests:

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/chi-strings.png"/>

If about 5 runs fail the test an 95 pass it, we can be very confident that the hash function is indeed uniformly distributed. For random inputs it is relatively easy though, so we also tested other input distribution, e.g. increasing integers:

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/chi-ints.png"/>

Here the LCG is too evenly distributed (due to its modulo arithmetics) which is a good thing here, but shows, that LCGs do not have a random uniform distribution.

The performance optimization of using two hash functions and combining them through the formula hash_i = hash1 + i * hash2 as suggested by <a href="https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf">Kirsch and Mitzenmacher</a> is theoretically sound as asymptotically hash values are perfectly uniform given to perfect hash values. In practice however, the distribution grows uneven for some inputs (the Cassandra team which uses this trick should have a look at that).

Now a real example of inserting random words in the Bloom filter with the resulting false positive rate after 30000 inserted elements demanding a false positive probability of 0.01:

<table><tr><th>Hash function</th><th>Speed  (ms)</th><th>f during insert (%)</th><th>f final (%)</th></tr>
<tr><td>RNG</td><td>80.529251</td><td>0,138</td><td>1,024</td></tr>
<tr><td>CarterWegman</td><td>1007.66743</td><td>0,199</td><td>0,956</td></tr>
<tr><td>SecureRNG</td><td>309.619582</td><td>0,185</td><td>0,902</td></tr>
<tr><td>CRC32</td><td>67.484589</td><td>0,121</td><td>1,007</td></tr>
<tr><td>Adler32</td><td>83.327968</td><td>10,074</td><td>22,539</td></tr>
<tr><td>Murmur2</td><td>100.20518</td><td>0,175</td><td>1,047</td></tr>
<tr><td>Murmur3</td><td>100.243902</td><td>0,189</td><td>0,993</td></tr>
<tr><td>Murmur3KirschMitzenmacher</td><td>45.999454</td><td>0,162</td><td>0,852</td></tr>
<tr><td>FNVWithLCG</td><td>42.25881</td><td>0,145</td><td>0,960</td></tr>
<tr><td>MD2</td><td>3635.465565</td><td>0,138</td><td>0,869</td></tr>
<tr><td>MD5</td><td>207.823532</td><td>0,148</td><td>0,936</td></tr>
<tr><td>SHA1</td><td>213.755936</td><td>0,189</td><td>0,923</td></tr>
<tr><td>SHA256</td><td>223.060422</td><td>0,165</td><td>1,054</td></tr>
<tr><td>SHA384</td><td>176.003345</td><td>0,165</td><td>0,832</td></tr>
<tr><td>SHA512</td><td>172.444648</td><td>0,152</td><td>1,064</td></tr>
</table>

In summary, cryptographic hash functions offer the most consistent uniform distribution, but are slightly more expensive to compute. LCGs, for instance Java Random, perform quite well in most cases and are cheap to compute. The best compromise seems to be the [Murmur 3 hash function](https://sites.google.com/site/murmurhash/), which has a good distribution and is quite fast to compute.

It's also possible to provide a custom hash function:
```java
BloomFilter<String> bf = new FilterBuilder(1000, 0.01)
    .hashFunction((value, m, k) -> null)
    .buildBloomFilter();
```


<a name="a7"/>
## Performance
To get meaningful results, the Bloom filters should be tested on machines where they are to be run. The test package contains a benchmark procedure (the test packages relies on the Apache Commons Math library):

```java
//Test the performance of the in-memory Bloom filter
BloomFilter<String> bf = new FilterBuilder(100_000, 0.01).hashFunction(HashMethod.Murmur3).buildBloomFilter();
MemoryBFTest.benchmark(bf, "Normal Bloom Filter", 1_000_000);
```
This gives over 2 Mio operations per second (on my laptop):
```
Normal Bloom Filter
hashes = 7 falsePositiveProbability = 3.529780138533512E-281 expectedElements = 1000000 size = 958506
add(): 0.687s, 1455604.0757 elements/s
addAll(): 0.47s, 2127659.5745 elements/s
contains(), existing: 0.472s, 2118644.0678 elements/s
contains(), nonexisting: 0.445s, 2247191.0112 elements/s
100000 hash() calls: 0.008s, 1.25E7 elements/s
Hash Quality (Chi-Squared-Test): p-value = 0.8041807628127277 , Chi-Squared-Statistic = 957318.7388845441
```

The Redis-backed and Counting Bloom filters can be tested similarly.

<a name="overview">
## Overview of Probabilistic Data Structures

<table style="font-size: 80%;">
  <tr>
    <th>Data Structure</th>
    <th>Set membership: „Have I seen this item before?“</th>
    <th>Frequency estimation: “How many of this kind have I seen?”</th>
    <th>Cardinality estimation: “How many distinct items have I seen in total”</th>
    <th>Item removal</th>
    <th>Persistence and distributed access</th>
  </tr>
  <tr>
    <td>Memory Bloom Filter</td>
    <td>Yes, with configurable false positive probability, O(1)</td>
    <td>No</td>
    <td>Yes, O(#bits)</td>
    <td>No</td>
    <td>No</td>
  </tr>
  <tr>
    <td>Memory Counting Bloom Filter</td>
    <td>Yes, with configurable false positive probability, O(1)</td>
    <td>Yes (Minimum Selection Algorithm), O(1)</td>
    <td>Yes, O(#bits)</td>
    <td>Yes, O(1)</td>
    <td>No</td>
  </tr>
  <tr>
    <td>Redis Bloom Filter</td>
    <td>Yes, with configurable false positive probability, O(1), single roundtrip, scalable through read slaves</td>
    <td>No</td>
    <td>Yes, O(#bits), single roundtrip, scalable through read slaves</td>
    <td>No</td>
    <td>Yes, configurable Redis persistence &amp; replication</td>
  </tr>
  <tr>
    <td>Redis Counting Bloom Filter</td>
    <td>Yes, with configurable false positive probability, O(1) , single roundtrip, scalable through read slaves</td>
    <td>Yes (Minimum Selection Algorithm), O(1) , single roundtrip, scalable through read slaves</td>
    <td>Yes, O(#bits), single roundtrip, scalable through read slaves</td>
    <td>Yes, O(1), in average 2 roundtrips</td>
    <td>Yes, configurable Redis persistence &amp; replication</td>
  </tr>
  <tr>
    <td>Other sketches (not part of this lib)</td>
    <td><a href="https://en.wikipedia.org/wiki/Hash_table#Sets">Hashsets</a>, <a href="https://en.wikipedia.org/wiki/Bit_array">Bitvectors</a>, <a 
    href="http://conferences2.sigcomm.org/co-next/2014/CoNEXT_papers/p75.pdf">Cuckoo 
    Filter</a></td>
    <td><a href="http://db.cs.berkeley.edu/cs286/papers/synopses-fntdb2012.pdf">Count-Min-Sketch, 
    Count-Mean-Sketch</a></td>
    <td><a href="http://research.neustar.biz/2012/07/09/sketch-of-the-day-k-minimum-values/">K-Minimum-Values</a>, 
    <a href="http://www.dmtcs.org/dmtcs-ojs/index.php/proceedings/article/viewPDFInterstitial/dmAH0110/2100">HyperLogLog</a></td>
    <td></td>
    <td></td>
  </tr>
</table>


Up next
==========
- Compatible Javascript implementation which can consume the JSON Bloom filter representation
- Redis Bloom filters that leverage Redis Cluster (once it's ready)

License
=======
This Bloom filter library is published under the very permissive MIT license, see LICENSE file.
