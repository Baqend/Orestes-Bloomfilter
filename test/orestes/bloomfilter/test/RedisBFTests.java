package orestes.bloomfilter.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CBloomFilter;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CBloomFilterRedis;
import orestes.bloomfilter.redis.CBloomFilterRedisBits;
import orestes.bloomfilter.redis.RedisBitSet;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisBFTests {

	public static String host = "192.168.44.131";
	public static int port = 6379;

	@Test
	public void basics() {
		Jedis jedis = jedis();
		double n = 2;
		double p = 0.01;
		CBloomFilterRedis<String> b = new CBloomFilterRedis<String>(host, port, n, p);
		CBloomFilterRedisBits<String> b2 = new CBloomFilterRedisBits<String>(host, port, n, p, 4);
		b.clear();
		b2.clear();
		BFTests.countingBasics(b);
		BFTests.countingBasics(b2);
		b.clear();
		b2.clear();
	}

	@Test
	public void normal() {
		Jedis jedis = jedis();
		double n = 5;
		double p = 0.01;
		CBloomFilterRedis<String> b = new CBloomFilterRedis<String>(host, port, n, p);
		CBloomFilterRedisBits<String> b2 = new CBloomFilterRedisBits<String>(host, port, n, p, 4);
		b.clear();
		b2.clear();
		BFTests.countingTest(b);
		BFTests.countingTest(b2);
		b.clear();
		b2.clear();
	}

	@Test
	public void ImplementationDifferences() {
		int n = 10_000;
		int m = 10_000;
		BloomFilter<String> bf = new BloomFilter<String>(m, 10);
		BloomFilterRedis<String> bfr = new BloomFilterRedis<String>(host, port, m, 10);
		CBloomFilter<String> cbf = new CBloomFilter<String>(m, 10, 4);
		CBloomFilterRedis<String> cbfr = new CBloomFilterRedis<String>(host, port, m, 10);
		CBloomFilterRedisBits<String> cbfrb = new CBloomFilterRedisBits<String>(host, port, m, 10, 4);

		bfr.clear();
		cbfr.clear();
		cbfrb.clear();

		BFTests.benchmark(bf, "Bloomfilter, Cryptographic - MD5", n);
		BFTests.benchmark(bf, "Bloomfilter, Cryptographic - MD5", n);
		BFTests.benchmark(cbf, "Bloomfilter, Cryptographic - MD5", n);
		BFTests.benchmark(bfr, "Redis Bloomfilter, Cryptographic - MD5", n);
		BFTests.benchmark(cbfr, "Redis Counting Bloomfilter, Cryptographic - MD5", n);
		BFTests.benchmark(cbfrb, "Redis Bit Counting Bloomfilter, Cryptographic - MD5", n);
		bfr.clear();
		cbfr.clear();
		cbfrb.clear();
	}

	//@Ignore
	@Test
	public void concurrencyTests() throws InterruptedException {
		final int n = 300;
		int threads = 2;
		new CBloomFilterRedis<String>(host, port, 10, 10).clear();
		final ArrayList<String> real = new ArrayList<String>(n);
		for (int i = 0; i < n; i++) {
			real.add("Ich bin die OID " + i);
		}
		// final CBloomFilter<String> cbfr = new CBloomFilter<String>(10000, 10, 4);
		Thread[] ts = new Thread[threads];
		for (int i = 0; i < threads; i++) {
			final int id = i;
			Runnable run = new Runnable() {

				@Override
				public void run() {
					CBloomFilter<String> cbfr = new CBloomFilterRedis<String>(host, port, 10, 10);
					for (int j = 0; j < n; j++) {
						String str = real.get((int) (Math.random() * n));
						String before = ((RedisBitSet)cbfr.getBitSet()).asBitSet().toString();
						cbfr.add(str);
						String between = ((RedisBitSet)cbfr.getBitSet()).asBitSet().toString();
						if (!cbfr.contains(str)) {
							//False Negative
							System.out.println("[Thread " + id + "]: ooops " + str + " not contained");
							System.out.println("Before: " + before);
							System.out.println("Between: " + between);
							System.out.println(Arrays.toString(cbfr.hash(str)));
						}
						cbfr.remove(str);
						if (cbfr.contains(str)) {
							//False Positive
							System.out.println("[Thread " + id + "]: ooops " + str + " still contained");
						}
					}
				}
			};
			Thread thread = new Thread(run);
			thread.start();
			ts[i] = thread;
		}
		for (int i = 0; i < threads; i++) {
			ts[i].join();
		}
	}

	private Jedis jedis() {
		return new Jedis(host, port);
	}
	
	public static void concurrentBenchmark(List<BloomFilter<String>> bfs, final int opsPerThread) {
		ExecutorService pool = Executors.newFixedThreadPool(bfs.size());
		List<Runnable> threads = new ArrayList<>(bfs.size());
		final List<String> items = new ArrayList<>(opsPerThread);
		for (int i = 0; i < opsPerThread; i++) {
			items.add(String.valueOf(i));
		}
		for(final BloomFilter<String> bf : bfs) {
			threads.add(new Runnable() {
				
				@Override
				public void run() {
					for (int i = 0; i < opsPerThread; i++) {
						bf.add(String.valueOf(i));
					}
//					for (int i = 0; i < opsPerThread; i++) {
//						bf.contains(String.valueOf(i));
//					}
//					bf.addAll(items);
				}
			});
		}
		long start = System.nanoTime();
		for(Runnable r : threads) {
			pool.execute(r);
		}
		pool.shutdown();
		try {
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			long end = System.nanoTime();
			System.out.println("Concurrent Benchmark, " + opsPerThread + " ops * " + bfs.size() + " threads = " + opsPerThread * bfs.size()  + " total ops: " + ((double) (end - start)) / 1000000 + " ms");
		} catch (InterruptedException e) {
			//...
		}
	}

}
