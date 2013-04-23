package orestes.bloomfilter.test;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CBloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;

import org.apache.commons.math.stat.inference.ChiSquareTestImpl;
import org.junit.Ignore;
import org.junit.Test;

public class BFTests {

	private static ArrayList<String> real;
	private static ArrayList<String> fake;
	private static ArrayList<String> hashOids;

	public static void plotHistogram(long[] histogram, String name) {
		System.out.println("Histogram for " + name + ":");
		long sum = 0;
		for (long bar : histogram)
			sum += bar;
		int index = 0;
		for (long bar : histogram) {
			double deviation = Math.abs(bar * 1.0 / sum - 1.0 / histogram.length);
			System.out.println("Index " + index++ + ": " + bar + ", Deviation from expectation: " + deviation);
		}
	}

	@Ignore
	@Test
	public void comparison() {
		int n = 100000;
		BloomFilter<String> bf = new BloomFilter<String>(10000, 10);
		bf.setHashMethod(HashMethod.Cryptographic);
		bf.setCryptographicHashFunction("MD5");
		benchmark(bf, "Cryptographic - MD5", n);
		bf.setCryptographicHashFunction("SHA-256");
		benchmark(bf, "Cryptographic - SHA-256", n);
		bf.setCryptographicHashFunction("SHA-512");
		benchmark(bf, "Cryptographic - SHA-512", n);
		bf.setCryptographicHashFunction("SHA1");
		benchmark(bf, "Cryptographic - SHA1", n);

		bf.setHashMethod(HashMethod.CarterWegman);
		benchmark(bf, "CarterWegmann", n);

		bf.setHashMethod(HashMethod.RNG);
		benchmark(bf, "Java Random", n);

		bf.setHashMethod(HashMethod.SecureRNG);
		benchmark(bf, "Java Secure Random", n);

		bf.setHashMethod(HashMethod.CRC32);
		benchmark(bf, "CRC32", n);
	}

	public static void benchmark(BloomFilter<String> bf, String configuration, int n) {
		int hashCount = 100000;

		System.out.println(configuration);
		System.out.print("k = " + bf.getK());
		System.out.print(" p = " + bf.getFalsePositiveProbability(n));
		System.out.print(" n = " + n);
		System.out.println(" m = " + bf.size());
		if (real == null || real.size() != n) {
			real = new ArrayList<String>(n);
			for (int i = 0; i < n; i++) {
				real.add("Ich bin die OID " + i);
			}

			hashOids = new ArrayList<String>(n);
			for (int i = 0; i < hashCount; i++) {
				hashOids.add("Ich bin die OID " + i);
			}

			fake = new ArrayList<String>(n);
			for (int i = 0; i < n; i++) {
				fake.add("Ich bin keine OID " + i);
			}
		}

		// Add elements
		System.out.print("add(): ");
		long start_add = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			bf.add(real.get(i));
		}
		long end_add = System.currentTimeMillis();
		printStat(start_add, end_add, n);

		// Bulk add elements
		System.out.print("addAll(): ");
		long start_add_all = System.currentTimeMillis();
		bf.addAll(real);
		long end_add_all = System.currentTimeMillis();
		printStat(start_add_all, end_add_all, n);

		// Check for existing elements with contains()
		System.out.print("contains(), existing: ");
		long start_contains = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			bf.contains(real.get(i));
		}
		long end_contains = System.currentTimeMillis();
		printStat(start_contains, end_contains, n);

		// Check for nonexisting elements with contains()
		System.out.print("contains(), nonexisting: ");
		long start_ncontains = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			bf.contains(fake.get(i));
		}
		long end_ncontains = System.currentTimeMillis();
		printStat(start_ncontains, end_ncontains, n);

		// Compute hashvalues
		System.out.print(hashCount + " hash() calls: ");
		int hashRounds = hashCount / bf.getK();
		long[] observed = new long[bf.getM()];
		long start_hash = System.currentTimeMillis();
		for (int i = 0; i < hashRounds; i++) {
			int[] hashes = bf.hash(hashOids.get(i));
			for (int h : hashes) {
				observed[h]++;
			}
		}

		// plotHistogram(observed, "Bench");

		long end_hash = System.currentTimeMillis();
		printStat(start_hash, end_hash, hashCount);

		double[] expected = new double[bf.getM()];
		for (int i = 0; i < bf.getM(); i++) {
			expected[i] = hashCount * 1.0 / bf.getM();
		}

		double pValue = 0;
		double chiSq = 0;
		ChiSquareTestImpl cs = new ChiSquareTestImpl();
		try {
			pValue = cs.chiSquareTest(expected, observed);
			chiSq = cs.chiSquare(expected, observed);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Hash Quality (Chi-Squared-Test): p-value = " + pValue + " , Chi-Squared-Statistic = "
				+ chiSq);
		System.out.println("");
		bf.clear();
	}

	public static void printStat(long start, long end, int n) {
		double diff = (end - start) / 1000.0;
		diff = (double) Math.round(diff * 10000) / 10000;
		double speed = (n / diff);
		speed = (double) Math.round(speed * 10000) / 10000;
		System.out.println(diff + "s, " + speed + " elements/s");
	}

	@Test
	public void normalTest() {
		int m = 26;
		int k = 3;
		BloomFilter<String> b = new BloomFilter<String>(m, k);
		normalTest(b);
		b = new BloomFilter<String>(m, k);
		b.setHashMethod(HashMethod.SimpeLCG);
		normalTest(b);
	}

	public static void normalTest(BloomFilter<String> b) {
		b.add("Käsebrot");
		b.add("ist");
		b.add("ein");
		b.add("gutes");
		b.add("Brot");
		assertTrue(b.contains("Käsebrot"));
		assertTrue(b.contains("ist"));
		assertTrue(b.contains("ein"));
		assertTrue(b.contains("gutes"));
		assertTrue(b.contains("Brot"));
		assertTrue(!b.contains("Kartoffelsalat"));
		assertTrue(!b.contains("Dachlatte"));
		assertTrue(!b.contains("Die Sechszehnte"));
		assertTrue(!b.contains("Die Secsdfhszehnte"));
		assertTrue(!b.contains("Die Saecsdfhszehnte"));
		assertTrue(!b.contains("Die Secsdfddhszehnte"));
		assertTrue(!b.contains("Die Secssdfhszehnte"));
		assertTrue(!b.contains("Die Sechszeaahnte"));
		assertTrue(!b.contains("Die Sechs34zehnte"));

		// False Positive:
		// assertTrue(!b.contains("Die Sechsjtzzehnte"));
	}

	@Test
	public void MD5performance() {
		int m = 100;
		int k = 1000000;
		String hashFunction = "MD5";
		BloomFilter<String> b = new BloomFilter<String>(m, k);
		b.setCryptographicHashFunction(hashFunction);
		long begin = System.nanoTime();
		long[] observed = new long[m];
		int[] hashValues = b.hash("Performance!");
		for (int i : hashValues) {
			observed[i]++;
		}
		long end = System.nanoTime();
		System.out
				.println("Time for calculating 1 million hashes with MD5: " + (end - begin) * 1.0 / 1000000000 + " s");
	}

	@Test
	public void SHAperformance() {
		int m = 100;
		int k = 1000000;
		String hashFunction = "SHA-512";
		BloomFilter<String> b = new BloomFilter<String>(m, k);
		b.setCryptographicHashFunction(hashFunction);
		long begin = System.nanoTime();
		long[] observed = new long[m];
		int[] hashValues = b.hash("Performance!");
		for (int i : hashValues) {
			observed[i]++;
		}
		long end = System.nanoTime();
		System.out.println("Time for calculating 1 million hashes with SHA-512: " + (end - begin) * 1.0 / 1000000000
				+ " s");
	}

	@Test
	public void countingTest() {
		double n = 5;
		double p = 0.01;
		CBloomFilter<String> b = new CBloomFilter<String>(n, p, 4);
		countingTest(b);
	}

	public static void countingTest(CBloomFilter<String> b) {
		System.out.println("Size of bloom filter: " + b.getM() + ", hash functions: " + b.getK());
		b.add("Käsebrot");
		b.add("ist");
		b.add("ein");
		b.add("gutes");
		b.add("Brot");
		assertTrue(b.contains("Käsebrot"));
		assertTrue(b.contains("ist"));
		assertTrue(b.contains("ein"));
		assertTrue(b.contains("gutes"));
		assertTrue(b.contains("Brot"));
		assertTrue(!b.contains("Kartoffelsalate"));
		assertTrue(!b.contains("Dachlatte"));
		assertTrue(!b.contains("Die Sechszehnte"));
		b.remove("Käsebrot");
		b.remove("ist");
		b.remove("ein");
		b.remove("gutes");
		b.remove("Brot");
		assertTrue(!b.contains("Käsebrot"));
		assertTrue(!b.contains("ist"));
		assertTrue(!b.contains("ein"));
		assertTrue(!b.contains("gutes"));
		assertTrue(!b.contains("Brot"));
	}

	@Test
	public void countingBasics() {
		double n = 2;
		double p = 0.01;
		CBloomFilter<String> b = new CBloomFilter<String>(n, p, 4);
		countingBasics(b);
	}

	public static void countingBasics(CBloomFilter<String> b) {
		System.out.println("Size of bloom filter: " + b.getM() + ", hash functions: " + b.getK());
		b.add("Käsebrot");
		assertTrue(b.contains("Käsebrot"));
		b.remove("Käsebrot");
		assertTrue(!b.contains("Käsebrot"));
		b.add("Schnitte");
		b.add("Schnitte");
		assertTrue(b.contains("Schnitte"));
		b.remove("Schnitte");
		assertTrue(b.contains("Schnitte"));
		b.remove("Schnitte");
		assertTrue(!b.contains("Schnitte"));
		CBloomFilter<String> bc = (CBloomFilter<String>) b.clone();
		assertTrue(b.equals(bc));
	}

	@Test
	public void normalPerformance() {
		int inserts = 100000;
		double n = 100000;
		double p = 0.02;
		BloomFilter<String> b = new BloomFilter<String>(n, p);
		CBloomFilter<String> cb = new CBloomFilter<String>(n, p, 4);
		System.out.println("Size of bloom filter: " + b.getM() + ", hash functions: " + b.getK());
		long begin = System.nanoTime();
		for (int i = 0; i < inserts; i++) {
			String value = "String" + i;
			b.add(value);
			cb.add(value);
		}
		long end = System.nanoTime();
		System.out.println("Total time for " + inserts
				+ " add operations in both a counting and a normal bloom filter: " + (end - begin) * 1.0 / 1000000000
				+ " s");
	}
}
