package orestes.bloomfilter.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class BFHashSpeed {
	private static ArrayList<ArrayList<String>> hashData;

	public static void main(String[] args) {
		// Calculated Hash Values
		int hashCount = 100_000;
		// Buckets
		int m = 1000;
		// Values per Hash functions
		int k = 5;
		// Successive runs
		int rounds = 10;
		long seed = 324234234;

		speedTests(hashCount, m, k, rounds, seed);
		hashCount = 1_000_000;
		speedTests(hashCount, m, k, rounds, seed);
	}

	public static void speedTests(int hashCount, int m, int k, int rounds, long seed) {
		String mathematica = "";

		List<List<String>> hashData = BFHashUniformity.generateHashData(hashCount, k, rounds, seed, BFHashUniformity.RAND);
		
		mathematica += testSpeed(HashMethod.Cryptographic, "MD5",hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.Cryptographic, "SHA-256",hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.Cryptographic, "SHA-512",hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.Cryptographic, "SHA1",hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.CarterWegman, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.RNG, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.SecureRNG, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.CRC32, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.Adler32, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.Murmur, null,hashData, hashCount, m, k, rounds, seed);
		mathematica += ",";
		mathematica += testSpeed(HashMethod.SimpeLCG, null,hashData, hashCount, m, k, rounds, seed);

		String speedVar = "speed" + hashCount + "h" + m + "m" + k + "k";
		String config = "hashes = " + hashCount + ", m = " + m + ", k = " + k + ", rounds = " + rounds;
		System.out.println(speedVar + " = { " + mathematica + "};");
		System.out.println("showSpeed[" + speedVar + ", \"Speed (ms)\\n" + config + "\"]");
	}

	public static String testSpeed(HashMethod hm, String cryptFunc, List<List<String>> hashData, int hashCount, int m, int k, int rounds, long seed) {
		DescriptiveStatistics speed = new DescriptiveStatistics();
		BloomFilter<String> b = new BloomFilter<String>(m, k);
		b.setHashMethod(hm);
		if (cryptFunc != null)
			b.setCryptographicHashFunction(cryptFunc);
		int hashRounds = hashCount / b.getK();

		Random r = new Random(seed);
		for (int i = 0; i < rounds; i++) {
			List<String> data = hashData.get(i);

			long start_hash = System.nanoTime();
			for (int i1 = 0; i1 < hashRounds; i1++) {
				int[] hashes = b.hash(data.get(i1));
			}
			long end_hash = System.nanoTime();

			speed.addValue((1.0 * end_hash - start_hash) / 1000000);
		}

		String cryptType = cryptFunc == null ? "" : " (" + cryptFunc + ")";
		System.out.println("HashMethod : " + hm + cryptType + ", " + " hashes = " + hashCount + ", m = " + m + ", k = "
				+ k + ", rounds = " + rounds);
		//System.out.println(speed);

		return Arrays.toString(speed.getValues()).replace("[", "{").replace("]", "}");
	}
	
}
