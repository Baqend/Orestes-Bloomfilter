Orestes Bloom filter library
===================

This is a set of Bloom filters we implemented as we found all existing open-source implementations to be lacking in various aspects. We will publish a statistical and performance analysis soon. Documentation on how to use these Bloom filters from a single applications as well as in a distributed, concurrent, Redis-based implementation will also follow.

### Regular Bloom Filter
The regular Bloom filter is very easy to use. It is the base class of all other Bloom filters. Figure out, how many elements you expect to have in the Bloom filter ( *n* ) and then which false positive rate is tolerable ( *p* ).

```java
//Create a Bloom filter that has a false positive rate of 0.1 when containing 1000 elements
BloomFilter<String> bf = new BloomFilter<>(1000, 0.1);
```
The Bloom filter class is generic and will work with any type that implements the `toString()` method in a sensible way, since that String is what the Bloom filter feeds into its hash functions. Now lets add something:

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

Also elements can be added in bulk:
```java
List<String> bulk = Arrays.asList(new String[] { "one", "two", "three" });
bf.addAll(bulk);
```

To get the best performance for a given use-case the parameters of the bloom filter mus be chosen wisely. There are several helpers and constructor overloads to configure the Bloom filter. So for example we could choose the Bloom filter to use 1000 Bits and then use the best number of hash functions for an expected amount of 6666 inserted elements. And then we choose Murmur as a different hash function:
```java
//Create a more customized Bloom filter
int m = 10000; //Bits to use
int k = BloomFilter.optimalK(6666, m); //Optimal number of hash functions given n and m
HashMethod hash = HashMethod.Murmur; //The hash function type
BloomFilter<Integer> bf2 = new BloomFilter<>(m, k);
//Only set the hash function before using the Bloom filter
bf2.setHashMethod(hash);
```


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
