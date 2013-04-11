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
The Javadoc is online [here](http://orestes-bloomfilter-docs.s3-website-eu-west-1.amazonaws.com/) and in the */docs* folder of the repository.

## Err, Bloom what?
Bloom filters are awesome data structures: **fast *and* maximally space efficient**.
```java
BloomFilter<String> urls = new BloomFilter<>(100_000_000, 0.01); //Expect 100M URLs
urls.add("http://github.com") //Add millions of URLs
urls.contains("http://twitter.com") //Know in an instant which ones you have or have not seen before
```
So what's the catch? Bloom filters allow false positive (i.e. URL contained though never added) with some  probability (0.01 in the example). If you can mitigate rare false positives (false negatives never happen) then Bloom filters are probably for you.

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
Download the [orestes-bf.jar](https://orestes-binaries.s3.amazonaws.com/orestes-bf.jar) and add it your classpath. The jar is also contained in the */build* folder of the repository. Or checkout the repository and build it using ant: `ant build`. For the normal Bloom filters it's even sufficient to only copy the source *.java files to your project.

## Usage
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

The good thing about the `union()` operation is, that it returns the exact Bloom filter which would have been created, if all elements were inserted in one Bloom filter.

There is a similar `intersect` operation that ANDs the Bit-Arrays. It does however behave slightly different as it does not return the Bloom filter that only contains the intersection. It guarantees to have all elements of the intersection but the false positive rate might be slightly higher than that of the pure intersection:

```java
other.add("this");
other.add("boggles");
one.intersect(other);
print(one.contains("this")); //true
print(one.contains("boggles")); //false
```

## Counting Bloom Filter


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
