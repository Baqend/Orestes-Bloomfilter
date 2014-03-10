package orestes.bloomfilter.test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.CustomHashFunction;
import orestes.bloomfilter.BloomFilter.HashMethod;
import orestes.bloomfilter.CBloomFilter;
import orestes.bloomfilter.CBloomFilter.OverflowHandler;
import orestes.bloomfilter.json.BloomFilterConverter;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CBloomFilterRedis;
import redis.clients.jedis.Jedis;

import com.google.gson.JsonElement;

public class Tutorial {
	
	public static void main(String[] args) throws Exception {
		testPerformance();
	}
	
	public static void regularBF() {
		//Create a Bloom filter that has a false positive rate of 0.1 when containing 1000 elements
		BloomFilter<String> bf = new BloomFilter<>(1000, 0.1);
		
		//Add a few elements
		bf.add("Just");
		bf.add("a");
		bf.add("test.");
		
		//Test if they are contained
		print(bf.contains("Just")); //true
		print(bf.contains("a")); //true
		print(bf.contains("test.")); //true
		
		//Test with a non-existing element
		print(bf.contains("WingDangDoodel")); //false
		
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
		//Compare with the expected amount
		print(bf.getFalsePositiveProbability(303) * 700); //1.74
		
		//Clone the Bloom filter
		bf.clone();
		//Reset it
		bf.clear();
		//add in Bulk
		List<String> bulk = Arrays.asList(new String[] { "one", "two", "three" });
		bf.addAll(bulk);
		print(bf.containsAll(bulk)); //true
		
		//Create a more customized Bloom filter
		//Bits to use
		int m = 10000;
		//Optimal number of hash functions
		int k = BloomFilter.optimalK(6666, m);
		//The hash function type
		HashMethod hash = HashMethod.Murmur;
		
		BloomFilter<Integer> bf2 = new BloomFilter<>(m, k);
		//Only set the hash function before using the Bloom filter
		bf2.setHashMethod(hash);
		
		//Create two Bloom filters with equal parameters
		BloomFilter<String> one = new BloomFilter<String>(100, 0.01);
		BloomFilter<String> other = new BloomFilter<String>(100, 0.01);
		one.add("this");
		other.add("that");
		one.union(other);
		print(one.contains("this")); //true
		print(one.contains("that")); //true
		
		other.add("this");
		other.add("boggles");
		one.intersect(other);
		print(one.contains("this")); //true
		print(one.contains("boggles")); //false
		
	}
	
	public static void countingBF() throws MalformedURLException{
		//Create a Counting Bloom filter that has a FP rate of 0.01 when 1000 are inserted
		//and uses 4 Bits for Counting
		CBloomFilter<String> cbf = new CBloomFilter<>(1000, 0.01, 4);
		cbf.add("http://google.com");
		cbf.add("http://twitter.com");
		print(cbf.contains("http://google.com")); //true
		print(cbf.contains("http://twitter.com")); //true
		
		//What only the Counting Bloom filter can do:
		cbf.remove("http://google.com");
		print(cbf.contains("http://google.com")); //false
		
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
		
		//See the inner structure
		CBloomFilter<String> small = new CBloomFilter<>(3, 0.2, 4);
		small.add("One");
		small.add("Two");
		small.add("Three");
		print(small.toString());
		
	}
	
	public static void redisBF() {
		//Redis' IP
		String IP = "192.168.44.132";	
		//Open a Redis-backed Bloom filter
		BloomFilterRedis<String> bfr = new BloomFilterRedis<>(IP, 6379, 10000, 0.01);
		bfr.add("cow");
		
		//Open a second Redis-backed Bloom filter with a new connection
		BloomFilterRedis<String> bfr2 = new BloomFilterRedis<>(IP, 6379, 10000, 0.01);
		bfr2.add("bison");
		
		print(bfr.contains("cow")); //true
		print(bfr.contains("bison")); //true
	}
	
	public static void redisCBF() {
		//Redis' IP
		String IP = "192.168.44.132";	
		//Open a Redis-backed Bloom filter
		CBloomFilterRedis<String> cbfr = new CBloomFilterRedis<>(IP, 6379, 10000, 0.01);
		cbfr.add("cow");
		
		//Open a second Redis-backed Bloom filter with a new connection
		CBloomFilterRedis<String> bfr2 = new CBloomFilterRedis<>(IP, 6379, 10000, 0.01);
		bfr2.add("bison");
		bfr2.remove("cow");
		
		print(cbfr.contains("bison")); //true
		print(cbfr.contains("cow")); //false
	}
	
	public static void jsonBF() {
		BloomFilter<String> bf = new BloomFilter<>(50, 0.1);
		bf.add("Ululu");
		JsonElement json = BloomFilterConverter.toJson(bf);
		print(json); //{"m":240,"k":4,"HashMethod":"Cryptographic","CryptographicHashFunction":"MD5","bits":"AAAAEAAAAACAgAAAAAAAAAAAAAAQ"}
		BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
		print(bf.contains("Ululu")); //true
	}
	
	public static void customHash() {
		BloomFilter<String> bf = new BloomFilter<>(1000, 0.01);
		bf.setCusomHashFunction(new CustomHashFunction() {
			@Override
			public int[] hash(byte[] value, int m, int k) {
				//...
				return null;
			}			
		});
	}
	
	public static void testPerformance() throws UnknownHostException, IOException {
//		//Test the performance of the in-memory Bloom filter
//		BloomFilter<String> bf = new BloomFilter<>(100_000, 0.01);
//		BFTests.benchmark(bf, "My test", 1_000_000);
//		
		//And the Redis-backed BF
		String IP = "192.168.44.132";	
//		//Open a Redis-backed Bloom filter
//		CBloomFilterRedisBits<String> cbfr = new CBloomFilterRedisBits<>(IP, 6379, 10000, 0.01, 4);
//		BFTests.benchmark(cbfr, "Redis Test", 10_000);
		
		List<BloomFilter<String>> bfs = new ArrayList<>();
		BloomFilterRedis<String> first = new BloomFilterRedis<String>(IP, 6379, 1000, 0.01);
		bfs.add(first);
		for (int i = 1; i < 40; i++) {
			bfs.add(new BloomFilterRedis<String>(new Jedis(IP, 6379)));
		}
		RedisBFTests.concurrentBenchmark(bfs, 2000);
	}
	
	private static <T> void print(T msg) {
		System.out.println(msg);
	}

}
