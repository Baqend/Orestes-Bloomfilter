Orestes Bloom filter library
===================

This is a set of Bloom filters we implemented as we found all existing open-source implementations to be lacking in various aspects. This libary takes some inspiration from the [simple Bloom filter implementation of Magnus Skjegstad](https://github.com/MagnusS/Java-BloomFilter) and the [Ruby Bloom filters by Ilya Grigorik](https://github.com/igrigorik/bloomfilter-rb).

The Bloom filter is a probabilistic set data structure which is very small. This is achieved by allowing false positives with some probability *p*. It has an `add` and `contains` operation which both are very fast (time complexity *O(1)*). The Counting Bloom filter is an extension of the Bloom filter with a `remove` operation at the cost of incurring an additional space overhead for counting. There are many good introductions to Bloom filters: the [Wikipedia article](http://en.wikipedia.org/wiki/Bloom_filter) is excellent, and even better is a [survey by Broder and Mitzenmacher](http://www.cs.utexas.edu/~yzhang/teaching/cs386m-f8/Readings/im2005b.pdf). Typical use cases of Bloom filters are content summaries and sets that would usually grow too large in fields such as networking, distributed systems, databases and analytics.

There are 4 types of Bloom filters in the Orestes Bloom filter library:
* **Regular Bloom filter**, a regular in-memory Java Bloom filter (`BloomFilter`)
* **Counting Bloom filter**, a Counting Bloom Filter which supports element removal (`CBloomFilter`)
* **Redis Bloom Filter**, a Redis-backed Bloom filter which can be concurrently used by different applications (`BloomFilterRedis`)
* **Redis Counting Bloom Filter**, a Redis-backed, concurrency-safe Counting Bloom filter in two variants: one that holds a pregenerated regular Bloom filter and relies on Redis Lua scripting (`CBloomFilterRedisBits`) and one that can be distributed through client side consistent hasing or Redis Cluster (`CBloomFilterRedis`)

### Docs
The Javadocs are online [here](http://orestes-bloomfilter-docs.s3-website-eu-west-1.amazonaws.com/) and in the */docs* folder of the repository.

## Err, Bloom what?
Bloom filters are awesome data structures: **fast *and* maximally space efficient**.
```java
BloomFilter<String> urls = new BloomFilter<>(100_000_000, 0.01); //Expect 100M URLs
urls.add("http://github.com") //Add millions of URLs
urls.contains("http://twitter.com") //Know in an instant which ones you have or have not seen before
```
So what's the catch? Bloom filters allow false positive (i.e. URL contained though never added) with some  probability (0.01 in the example). If you can mitigate rare 

false positives (false negatives never happen) then Bloom filters are probably for you.

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

## Getting started
Download the [orestes-bf.jar](https://orestes-binaries.s3.amazonaws.com/orestes-bf.jar) and add it your classpath. The jar is also contained in the */build* folder of the repository. Or checkout the repository and build it using ant: `ant build`. For the normal Bloom filters it's even sufficient to only copy the source *.java files to your  project.

## Usage
- [Regular Bloom Filter](#a1)
- [Counting Bloom Filter](#a2)
- [Redis Bloom Filters](#a3)
- [Redis Counting Bloom Filters](#a4)
- [JSON Representation](#a5)

<a name="a1"/>
### Regular Bloom Filter
The regular Bloom filter is very easy to use. It is the base class of all other Bloom filters. Figure out how many elements you expect to have in the Bloom filter ( *n* ) and then which false positive rate is tolerable ( *p* ).

```java
//Create a Bloom filter that has a false positive rate of 0.1 when containing 1000 elements
BloomFilter<String> bf = new BloomFilter<>(1000, 0.1);
```
The Bloom filter class is generic and will work with any type that implements the `toString()` method in a sensible way, since that String is what the Bloom filter feeds into its hash functions. The `hashCode()` method is not used, since it returns integers that normally do not satisfy a uniform distribution of outputs that is essential for the optimal peformance of the Bloom filter. Now lets add something:

```java
//Add a few elements
bf.add("Just");
bf.add("a");
bf.add("test.");
```

An element which was inserted in a Bloom filter will always be returned as being contained (no false negatives):

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

To get the best performance for a given use-case the parameters of the bloom filter must be chosen wisely. There are several helpers and constructor overloads to configure the Bloom filter. So for example we could choose the Bloom filter to use 1000 Bits and then use the best number of hash functions for an expected amount of 6666 inserted elements. We choose Murmur as our hash function which is faster than cryptographic hash functions like MD5:
```java
//Create a more customized Bloom filter
int m = 10000; //Bits to use
int k = BloomFilter.optimalK(6666, m); //Optimal number of hash functions given n and m
HashMethod hash = HashMethod.Murmur; //The hash function type
BloomFilter<Integer> bf2 = new BloomFilter<>(m, k);
//Only set the hash function before using the Bloom filter
bf2.setHashMethod(hash);
```

Bloom filters allow other cool stuff too. Consider for instance that you collected two Bloom filters which are compatible in their parameters. Now you want to consolidate their elements. This is achieved by ORing the respective Bit-Arrays of the Bloom filters:
```java
//Create two Bloom filters with equal parameters
BloomFilter<String> one = new BloomFilter<String>(100, 0.01);
BloomFilter<String> other = new BloomFilter<String>(100, 0.01);
one.add("this");
other.add("that");
one.union(other);
print(one.contains("this")); //true
print(one.contains("that")); //true
```

The good thing about the `union()` operation is, that it returns the exact Bloom filter which would have been created, if all elements were inserted in one Bloom filter. There is a similar `intersect` operation that ANDs the Bit-Arrays. It does however behave slightly different as it does not return the Bloom filter that only contains the 
intersection. It guarantees to have all elements of the intersection but the false positive rate might be slightly higher than that of the pure intersection:

```java
other.add("this");
other.add("boggles");
one.intersect(other);
print(one.contains("this")); //true
print(one.contains("boggles")); //false
```

<a name="a2"/>
## Counting Bloom Filter
The Counting Bloom filter allows object removal. For this purpose it has binary counters instead of simple bits. In `CBloomFilter` the amount of bits *c* per counter can be set. If you expect to insert elements only once, the probability of a Bit overflow is very small for *c = 4* : *1.37 * 10^-15 * m* for up to *n* inserted elements  ([details](http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html#SECTION00053000000000000000)). For most use-cases 4 Bits are the best choice.

```java
//Create a Counting Bloom filter that has a FP rate of 0.01 when 1000 are inserted
//and uses 4 Bits for Counting
CBloomFilter<String> cbf = new CBloomFilter<>(1000, 0.01, 4);
cbf.add("http://google.com");
cbf.add("http://twitter.com");
print(cbf.contains("http://google.com")); //true
print(cbf.contains("http://twitter.com")); //true
```

If you insert one distinct item multiple times, the same counter always get updated so you should pick a higher *c* so that *2^c > inserted_copies*. The Counting Bloom Filter extends the normal Bloom Filter by `remove` and `removeAll` methods:

```java
cbf.remove("http://google.com");
print(cbf.contains("http://google.com")); //false
```

To handle overflows (which is unlikely to ever be an issue) you can set an overflow callback:

```java
//Handling Overflows
cbf.setOverflowHandler(new OverflowHandler() {
	@Override
	public void onOverflow() {
		print("Oops, c should better be higher the next time.");
	}
});
for (int i = 1; i < 20; i++) {
	print("Round " + i);
	cbf.add("http://example.com"); //Causes onOverflow() in Round >= 16
}
```

To understand the inner workings of the Counting Bloom filter lets actually look at the bits of small filter:

```java
CBloomFilter<String> small = new CBloomFilter<>(3, 0.2, 4);
small.add("One"); small.add("Two"); small.add("Three");
print(small.toString());
```
This gives:
```bash
Counting Bloom Filter, Parameters m = 11, k = 3, c = 4
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

The Counting Bloom filter thus has a bit size of 11, uses 3 hash functions and 4 bits for counting. The first row is the materialized bit array of all counters > 0. Explcitly saving it makes `contains` calls fast and generation when transferring the Counting Bloom Filter flattened to a Bloom filter.


<a name="a3"/>
## Redis Bloom Filters
Bloom filters are really intresting beauce they allow very high throughput and minimal latency for adding and querying (and removing). Therefore you might want to use them across the boundaries of a single machine. For instance imagine you run a large scale web site or web service. You have a load balancer distributing the request load over several front-end web servers. You now want to store some information with a natural set structure, say, you want to know if a source IP adress has accessed the requested URL in the past. You could achieve that by either eplicitly storing that information (probably in a database) which will soon be a bottleneck if you serve billions of requests a day. Or you employ a shared Bloom filter and accept a small possibility of false positives.

These kind of use-cases are ideal for the Redis-backed Bloom filters of this library. They have the same Java Interfaces as the normal and Counting Bloom filter but store the Bloom filter bits in the [in-memory key-value store Redis](http://redis.io).

Reasons to use these Redis-backed Bloom filters instead of their pure Java brothers are:
* **Concurrent** or **Distributed** Access to on Bloom filter
* **Persistence** Requirements (e.g. saving the Bloom filter to disk once every second)
* **Scalability** of the Bloom Filter beyond one machine ([Redis Cluster](http://redis.io/topics/cluster-spec) or client-side sharding with *CBlommFilterRedisSharded* which is under development )

Using the Redis-backed Bloom filter is straightforward:

1. Install Redis. This is extremely easy: [see Redis Installation](http://redis.io/download).
2. Start Redis with `$ redis-server`. The server will listen on port 6379.
3. In your application (might be on a different machine) instantiate a Redis-backed Bloom filter giving the IP or host name of Redis and its port: `new BloomFilterRedis<>("192.168.44.131", 6379, 10000, 0.01);`

The Redis-backed Bloom filters have the same Interface as the normal Bloom filters:

```java
//Redis' IP
String IP = "192.168.44.131";	
//Open a Redis-backed Bloom filter
BloomFilterRedis<String> bfr = new BloomFilterRedis<>(IP, 6379, 10000, 0.01);
bfr.add("cow");

//Open a second Redis-backed Bloom filter with a new connection
BloomFilterRedis<String> bfr2 = new BloomFilterRedis<>(IP, 6379, 10000, 0.01);
bfr2.add("bison");

print(bfr.contains("cow")); //true
print(bfr.contains("bison")); //true
```

The Redis-backed Bloom filters are concurrency/thread-safe at the backend. That means you can concurrently insert from any machine without running into anomalies, inconsistencies or lost data. The Redis-backed Bloom filters are implemented using efficient [Redis bit arrays](http://redis.io/commands/getbit). They make heavy use of [pipelining](http://redis.io/topics/pipelining) so that every `add` and `contains` call only needs one round-trip. This is the most performance critical aspect and usually not found in [other implementations](https://github.com/igrigorik/bloomfilter-rb) which need one round-trip for every Bit or worse.

The Redis-backed Bloom filters save their metadata (like number and kind of hash functions) in Redis, too. Thus other clients can easily to connect to a Redis instance that already holds a Bloom filter using `new BloomFilterRedis(new Jedis(ip, port))` or the similar constructors of *CBloomFilterRedis* or *CBloomFilterRedisBits*.

<a name="a4"/>
## Redis Counting Bloom Filters
There are two kinds of Redis-backed Counting Bloom filters:
* **CBloomfilterRedis** saves the counters as separate keys and keeps the materialized flat Bloom filter as bit array. It is comptatible with Redis 2.4 or higher.
* **CBloomFilterRedisBits** uses a bit array for the counters. This is much more space efficient (~ factor 10) but needs at least Redis 2.6 as it relies on Lua scripting.

```java
CBloomFilterRedis<String> cbfr = new CBloomFilterRedis<>(IP, 6379, 10000, 0.01);
cbfr.add("cow");
CBloomFilterRedis<String> bfr2 = new CBloomFilterRedis<>(IP, 6379, 10000, 0.01);
bfr2.add("bison");
bfr2.remove("cow");
print(cbfr.contains("bison")); //true
print(cbfr.contains("cow")); //false
```

*CBloomFilterRedisBits* works similar but you have to provide the counter size *c*. It uses Lua scripts (stored procedures in Redis) to atomically increment and decrement on a bit array. If you use Redis 2.6 or higher and it runs on one machine, you should choose *CBloomFilterRedisBits* as it consumes much less memory.

<a name="a5"/>
## JSON Representation
To easily transfer a Bloom filter to a client (for instance via an HTTP GET) there is a JSON Converter for the Bloom filters are implemented so that this generation option is very cheap (i.e. just sequntially reading it from memory). It works for all Bloom filters including the ones backed by Redis.
```java
BloomFilter<String> bf = new BloomFilter<>(50, 0.1);
bf.add("Ululu");
JsonElement json = BloomFilterConverter.toJson(bf);
print(json); //{"m":240,"k":4,"HashMethod":"Cryptographic","CryptographicHashFunction":"MD5","bits":"AAAAEAAAAACAgAAAAAAAAAAAAAAQ"}
BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
print(bf.contains("Ululu")); //true
```
JSON is not an ideal format for binary content (Base64 only uses 64 out of 94 possible characters) it's highly interoperable and easy to read which outweighs the slight waste of space. Combining it with a [Content-Encoding](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html) like gzip usually compensates that.

<a name="a6"/>
## Hash Functions
There is a detailed description of the available hash functions in the <a href="http://orestes-bloomfilter-docs.s3-website-eu-west-1.amazonaws.com/orestes/bloomfilter/BloomFilter.html#setHashMethod(orestes.bloomfilter.BloomFilter.HashMethod)">Javadocs of the Bloomfilter.setHashMethod method</a> and the Javadocs of the respective hash function implementations. Hash uniformity (i.e. all bits of the Bloom filter are equally likely) is of great importance for the false positive rate. But there is also an inherent tradeoff between hash uniformity and speed of computation. For instance cryptographic hash functions have very good distribution properties but are very CPU intensive. Pseudorandom number generators like the [linear congruential generator](http://en.wikipedia.org/wiki/Linear_congruential_generator) are easy to compute but do not have perfectly random outputs but rather certain distribution patterns which for some inputs are notable and for others are negligible.

Here is a Box plot overview of how good the different hash functions perform (Intel i7 w/ 4 cores, 8 GB RAM). The configuration is 1000000 hashes using k = 5, m = 1000 averaged over 10 runs. 

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/hash-speed.png"/>

Speed of computation doesn't tell much about the quality of hash values. A good hash function is one, which has a discrete uniform distribution of outputs. That means that every bit of the Bloom filter's bit vector is equally likely to bet set. To measure, if and how good the hash functions follow a uniform distribution we did [goodness of fit Chi-Square hypothesis tests](http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test).

Here are some of the results. The inputs are random strings. The p-value is the probability of getting a statistical result that is at least as extreme as the obtained result. So the usual way of hypothesis testing would be rejecting the null hypothesis ("the hash hash function is uniformly distributed") if the p-value is smaller than 0.05. We did 100 Chi-Square Tests:

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/chi-strings.png"/>

If about 5 runs fail the test an 95 pass it, we can be very confident that the hash function is indeed uniformly distributed. For random inputs it is relatively easy though, so we also tested other input distribution, e.g. increasing integers:

<img src="https://orestes-bloomfilter-images.s3-external-3.amazonaws.com/chi-ints.png"/>

Here the LCG is too evenly distributed (due to its modulo arithmetics) which is a good thing here, but shows, that LCGs do not have a random uniform distribution. The Carter-Wegman hash function fails because its constants are too small.

Now a real example of inserting random elements in the Bloom filter, ordered by the false postive rate after  n = 30000 inserted elements using m = 300000 bits:

<table>
<tr><th>Hashfunction</th><th>Speed  (ms)</th><th>f during insert</th><th>f final</th></tr>
 <tr><td>SimpleLCG</td><td>26,83</td><td>0,0016</td><td>0,0095</td></tr>
 <tr><td>Cryptographic (MD5)</td><td>247,8</td><td>0,0017</td><td>0,0097</td></tr>
 <tr><td>Java RNG</td><td>37,05</td><td>0,0013</td><td>0,0101</td></tr>
 <tr><td>SecureRNG</td><td>263,2</td><td>0,0013</td><td>0,0101</td></tr>
 <tr><td>CarterWegman</td><td>606,76</td><td>0,0013</td><td>0,0101</td></tr>
 <tr><td>CRC32</td><td>114,4</td><td>0,0012</td><td>0,0102</td></tr>
 <tr><td>Cryptographic (SHA-256)</td><td>265,28</td><td>0,0012</td><td>0,0103</td></tr>
 <tr><td>Murmur</td><td>57,94</td><td>0,0017</td><td>0,0104</td></tr>
 <tr><td>Cryptographic (SHA1)</td><td>257,99</td><td>0,0016</td><td>0,0105</td></tr>
 <tr><td>Cryptographic (SHA-512)</td><td>232,22</td><td>0,0015</td><td>0,0111</td></tr>
 <tr><td>Adler32</td><td>116,73</td><td>0,9282</td><td>0,986</td></tr>
</table>

In summary, cryptographic hash functions offer the most consistent uniform distribution, but are slightly more expensive to compute. LCGs, for instance Java Random, perform quite well in most cases and are cheap to compute. The best compromise seems to be the [Murmur hash function](https://sites.google.com/site/murmurhash/), which has a good distribution and is quite fast to compute.

Performance
===========

Next steps
==========
- Compatible Javascript implementation which can consume the JSON Bloom filter representation
- *CBloomFilterSharded* which just uses counters as keys without a materialized bit array. It will have no hotspots and use client-side sharding to distribute keys over an 

arbitrary amount of Redis instances. The trade-off is unlimited horizontal scalability vs inefficient generation of the flat Bloom filter.

License
=======
This Bloom filter library is published under the very permissive MIT license:

Copyright Felix Gessert and Florian Bücklers. All rights reserved.
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
