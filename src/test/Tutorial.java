package test;

import java.util.Arrays;
import java.util.List;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;

public class Tutorial {
	
	public static void main(String[] args) {
		regularBF();
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
	
	private static <T> void print(T msg) {
		System.out.println(msg);
	}

}
