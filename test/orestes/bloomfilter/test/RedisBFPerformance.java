package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;
import orestes.bloomfilter.redis.CBloomFilterRedisBits;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class RedisBFPerformance {
	public static void main(String[] args) {
		String host = RedisBFTest.host;
		if(args.length > 0)
			host = args[0];
		int items = 100_000_000;
		int count = 30_000;
		int m = 100_000;
		int k = 10;
		compareToRuby(host, count, m, k, items);
	}

	private static void compareToRuby(String host, int count, int m, int k, int items) {
		long start = System.currentTimeMillis();
		//BloomFilter<String> b = new BloomFilter<String>(m, k);
		BloomFilter<String> b = new CBloomFilterRedisBits<String>(host, RedisBFTest.port, m, k, 4);
		//BloomFilter<String> b = new CBloomFilterRedis<String>(RedisBFTests.host, RedisBFTests.port, m, k, 4);
		b.clear();
		b.setHashMethod(HashMethod.Murmur);
		//b.setCryptographicHashFunction("SHA-256");
		Random r = new Random();
		int fp = 0;
		Set<String> seen = new HashSet<String>();
		for (int i = 0; i < count; i++) {
			String element = String.valueOf(r.nextInt(items));
			if (b.contains(element) && !seen.contains(element))
				fp++;
			b.add(element);
			seen.add(element);
		}
		double fprate = 100.0 * fp / count;
		System.out.println("False Positives = " + fp + ", FP-Rate = " + fprate);
		long end = System.currentTimeMillis();
		BFTest.printStat(start, end, count);
	}

}
