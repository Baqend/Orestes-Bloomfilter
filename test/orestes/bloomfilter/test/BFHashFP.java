package orestes.bloomfilter.test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;

/**
 * Allows to count actual False Positives.
 */
public class BFHashFP {
	
	public static void main(String[] args) {
		testHashing();
	}
	
	public static void testHashing() {
		int hashCount = 5000;
		int m = 10000;
		int k = 10;
		int rounds = 1;
		long seed = 324234234;
		double alpha = 0.05;
		int mode = BFHashUniformity.RAND;
		StringBuilder log = new StringBuilder();

		// for (int i = 0; i < 3; i++) {
		// mode = i;
		// testHashDistribution(hashCount, 100, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 1000, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 10000, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 10000, 5, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 100000, k, rounds, seed, alpha, log, mode);
		// //testHashDistribution(hashCount, 1000000, k, rounds, seed, alpha, log, mode);
		// }

		// testHashDistribution(100000, 10000, k, 100, seed, alpha, log, ID);

		for (int currentMode = 0; currentMode < 4; currentMode++) {
			testFalsePositives(1_000_00, 10_000_00, 10, seed, currentMode);
		}

		// hashCount = 1000000;
		// rounds = 1;
		// for (int i = 0; i < 3; i++) {
		// mode = i;
		// testHashDistribution(hashCount, 100, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 1000, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 10000, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 10000, 5, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 100000, k, rounds, seed, alpha, log, mode);
		// testHashDistribution(hashCount, 1000000, k, rounds, seed, alpha, log, mode);
		// }

		System.out.println(log.toString());
	}

	public static void testFalsePositives(int inserts, int m, int k, long seed, int mode) {
		BloomFilter<String> dummy = new BloomFilter<String>(m, k);
		String test = "False Positives, hashData = " + BFHashUniformity.modeName(mode) + ", inserts = " + inserts + ", m = " + m
				+ ", k = " + k + ", Expected final FP-Rate = " + dummy.getFalsePositiveProbability(inserts) * 100;
		System.out.println(test);
		for (int i = 0; i < test.length(); i++)
			System.out.print("-");
		System.out.println();

		List<String> hashData = BFHashUniformity.generateHashData(inserts, 1, 1, seed, mode).get(0);
		testFP(HashMethod.Cryptographic, "MD5", hashData, m, k, seed);
		testFP(HashMethod.Cryptographic, "SHA-256", hashData, m, k, seed);
		testFP(HashMethod.Cryptographic, "SHA-512", hashData, m, k, seed);
		testFP(HashMethod.Cryptographic, "SHA1", hashData, m, k, seed);
		testFP(HashMethod.CarterWegman, null, hashData, m, k, seed);
		testFP(HashMethod.RNG, null, hashData, m, k, seed);
		testFP(HashMethod.SecureRNG, null, hashData, m, k, seed);
		testFP(HashMethod.CRC32, null, hashData, m, k, seed);
		testFP(HashMethod.Adler32, null, hashData, m, k, seed);
		testFP(HashMethod.Murmur, null, hashData, m, k, seed);
		testFP(HashMethod.SimpeLCG, null, hashData, m, k, seed);
		testFP(HashMethod.Magnus, "MD5", hashData, m, k, seed);

		System.out.println();
	}

	public static void testFP(HashMethod hm, String cryptFunc, List<String> hashData, int m, int k, long seed) {
		BloomFilter<String> b = new BloomFilter<String>(m, k);
		b.setHashMethod(hm);
		if (cryptFunc != null)
			b.setCryptographicHashFunction(cryptFunc);
		String cryptType = cryptFunc == null ? "" : " (" + cryptFunc + ")";
		int inserts = hashData.size();
		int fps = 0;
		Set<String> seen = new HashSet<String>();
		long start = System.nanoTime();
		for (int i = 0; i < inserts; i++) {
			// Select random item
			String current = hashData.get(i);

			if (b.contains(current) && !seen.contains(current)) {
				fps++;
			}

			b.add(current);
			seen.add(current);
		}
		
		int totalfps = 0;
		int tested = 0;
		Random r = new Random();
		while(tested < inserts) {
			String current = String.valueOf(r.nextInt());
			if (!seen.contains(current)) {
				tested++;
				if(b.contains(current))
					totalfps++;
			}
		}
		
		long end = System.nanoTime();
		double speed = (1.0 * end - start) / 1000000;

		double fpRate = 100.0 * fps / inserts;
		double totalFpRate = 100.0 * totalfps / inserts;
		String result = "HashMethod : " + hm + cryptType + ", speed = " + speed + " ms, False-Positives = " + fps
				+ ", FP-Rate = " + fpRate + ", final FP-Rate = " + totalFpRate;
		System.out.println(result);
	}
}
