package orestes.bloomfilter.test;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.BloomFilter.HashMethod;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ChiSquaredDistributionImpl;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;

public class BFHashUniformity {
	public static int RAND = 0;
	public static int INTS = 1;
	public static int ID = 2;
	public static int RANDINTS = 3;

	
	public static void main(String[] args) {
		testHashing();
		//testMurmur();
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

		testHashDistribution(100_000, 10_000, k, 100, seed, alpha, log, ID);

		System.out.println(log.toString());
	}
	
	

	public static void testMurmur() {
		BloomFilter<String> b = new BloomFilter<String>(1000, 10);
		b.setHashMethod(HashMethod.Murmur);
		Random r = new Random();
		for (int i = 0; i < 1000; i++) {
			int seed = r.nextInt();
			System.out.println(seed);
			List<String> hashData = BFHashUniformity.generateHashData(1_000_000, 1, 1, seed, RAND).get(0);
			for(String s: hashData) {
				b.add(s);
				b.contains(s);
			}
		}
	}

	

	public static void testHashDistribution(int hashCount, int m, int k, int rounds, long seed, double alpha,
			StringBuilder log, int mode) {

		List<List<String>> hashData = generateHashData(hashCount, k, rounds, seed, mode);

		double crit = criticalVal(m, alpha);
		System.out.println("The critical Chi-Squared-Value for alpha = " + alpha + " is " + crit);

		// Format the data for processing in Mathematica
		StringBuilder chi = new StringBuilder();
		StringBuilder pvalue = new StringBuilder();

		testDistribution(HashMethod.Cryptographic, "MD5", hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.Cryptographic, "SHA-256", hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.Cryptographic, "SHA-512", hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.Cryptographic, "SHA1", hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.CarterWegman, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.RNG, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.SecureRNG, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.CRC32, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.Adler32, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.Murmur, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);
		testDistribution(HashMethod.SimpeLCG, null, hashCount, m, k, rounds, seed, hashData, chi, pvalue);

		// Remove trailing comma
		String chiMathematica = chi.toString();
		chiMathematica = chiMathematica.substring(0, chiMathematica.length() - 1);
		String pValueMathematica = pvalue.toString();
		pValueMathematica = pValueMathematica.substring(0, pValueMathematica.length() - 1);

		String dataSource = modeName(mode);

		String config = "hashes = " + hashCount + ", m = " + m + ", k = " + k + ", rounds = " + rounds
				+ ", hashData = " + dataSource;

		log.append("data" + hashCount + "h" + m + "m" + k + "k" + " = { " + chiMathematica + "};\n");
		log.append("crit" + hashCount + "h" + m + "m" + k + "k" + " = " + crit + ";\n");
		log.append("show[data" + hashCount + "h" + m + "m" + k + "k" + ", crit" + hashCount + "h" + m + "m" + k + "k"
				+ ", \"Chi-Squared Statistic\\n" + config + "\"]\n");

		log.append("pdata" + hashCount + "h" + m + "m" + k + "k" + " = { " + pValueMathematica + "};\n");
		log.append("pcrit" + hashCount + "h" + m + "m" + k + "k" + " = " + alpha + ";\n");
		log.append("show[pdata" + hashCount + "h" + m + "m" + k + "k" + ", pcrit" + hashCount + "h" + m + "m" + k + "k"
				+ ", \"p-Value\\n" + config + "\"]\n");

	}


	public static void testDistribution(HashMethod hm, String cryptFunc, int hashCount, int m, int k, int rounds,
			long seed, List<List<String>> hashData, StringBuilder chi, StringBuilder pvalue) {
		DescriptiveStatistics pValues = new DescriptiveStatistics();
		DescriptiveStatistics xs = new DescriptiveStatistics();

		BloomFilter<String> b = new BloomFilter<String>(m, k);
		b.setHashMethod(hm);
		if (cryptFunc != null)
			b.setCryptographicHashFunction(cryptFunc);
		int hashRounds = hashCount / b.getK();

		String cryptType = cryptFunc == null ? "" : " (" + cryptFunc + ")";

		for (int i = 0; i < rounds; i++) {
			List<String> data = hashData.get(i);

			long[] observed = new long[b.getM()];
			for (int j = 0; j < hashRounds; j++) {
				int[] hashes = b.hash(data.get(j));
				for (int h : hashes) {
					observed[h]++;
				}
			}
			// plotHistogram(observed, "Bench");

			double[] expected = new double[b.getM()];
			for (int j = 0; j < b.getM(); j++) {
				expected[j] = hashCount * 1.0 / b.getM();
			}

			ChiSquareTestImpl cs = new ChiSquareTestImpl();
			try {
				pValues.addValue(cs.chiSquareTest(expected, observed));
				xs.addValue(cs.chiSquare(expected, observed));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		String result = "HashMethod : " + hm + cryptType + ", " + " hashes = " + hashCount + ", m = " + m + ", k = "
				+ k + ", rounds = " + rounds;
		System.out.println(result);

		chi.append(stringify(xs.getValues()));
		pvalue.append("(*" + result + "*)");
		pvalue.append(stringify(pValues.getValues()));
	}

	// Get rid of the scientific 0.34234E8 notation
	public static String stringify(double[] values) {
		NumberFormat f = NumberFormat.getInstance(Locale.ENGLISH);
		f.setGroupingUsed(false);
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (double val : values) {
			sb.append(f.format(val));
			sb.append(",");
		}
		return sb.substring(0, sb.length() - 1) + "},";
	}

	public static List<List<String>> generateHashData(int hashCount, int k, int rounds, long seed, int mode) {
		List<List<String>> hashData = null;
		if (mode == RAND)
			hashData = generateHashData(hashCount, k, rounds, seed);
		else if (mode == INTS)
			hashData = generateHashDataInt(hashCount, k, rounds, seed);
		else if (mode == ID)
			hashData = generateHashDataIds(hashCount, k, rounds, seed);
		else if (mode == RANDINTS)
			hashData = generateHashDataRandomInt(hashCount, k, rounds, seed);
		return hashData;
	}
	
	public static String modeName(int mode) {
		String dataSource = null;
		if (mode == RAND)
			dataSource = "random strings";
		else if (mode == INTS)
			dataSource = "increasing integers";
		else if (mode == ID)
			dataSource = "increasing IDs";
		else if (mode == RANDINTS)
			dataSource = "random integers";
		return dataSource;
	}

	// Generate random 128 Bit Strings
	public static List<List<String>> generateHashData(int hashCount, int k, int rounds, long seed) {
		int hashRounds = hashCount / k;
		Random r = new Random(seed);

		List<List<String>> hashData = new ArrayList<List<String>>(rounds);
		for (int j = 0; j < rounds; j++) {
			hashData.add(new ArrayList<String>(hashRounds));
		}
		for (int i = 0; i < rounds; i++) {

			List<String> data = hashData.get(i);
			Set<String> seen = new HashSet<String>(hashRounds);
			for (int j = 0; j < hashRounds; j++) {
				byte[] bytes = new byte[16];
				r.nextBytes(bytes);
				String newString = new String(bytes);
				if (!seen.contains(newString))
					data.add(newString);
				seen.add(newString);
			}

		}
		return hashData;
	}

	// Generate unique random ints.
	public static List<List<String>> generateHashDataRandomInt(int hashCount, int k, int rounds, long seed) {
		int hashRounds = hashCount / k;
		Random r = new Random(seed);

		List<List<String>> hashData = new ArrayList<List<String>>(rounds);
		for (int j = 0; j < rounds; j++) {
			hashData.add(new ArrayList<String>(hashRounds));
		}
		for (int i = 0; i < rounds; i++) {

			List<String> data = hashData.get(i);
			Set<String> seen = new HashSet<String>(hashRounds);
			for (int j = 0; j < hashRounds; j++) {
				String newString = String.valueOf(r.nextInt());
				if (!seen.contains(newString))
					data.add(newString);
				seen.add(newString);
			}

		}
		return hashData;
	}

	// Generate increasing numbers
	public static List<List<String>> generateHashDataInt(int hashCount, int k, int rounds, long seed) {
		int hashRounds = hashCount / k;

		List<List<String>> hashData = new ArrayList<List<String>>(rounds);
		for (int j = 0; j < rounds; j++) {
			hashData.add(new ArrayList<String>(hashRounds));
		}

		for (int i = 0; i < rounds; i++) {

			List<String> data = hashData.get(i);
			for (int j = 0; j < hashRounds; j++) {
				data.add(String.valueOf(j + i * hashRounds));
			}

		}
		return hashData;
	}

	// Generate pseudo identifiers
	public static List<List<String>> generateHashDataIds(int hashCount, int k, int rounds, long seed) {
		int hashRounds = hashCount / k;

		List<List<String>> hashData = new ArrayList<List<String>>(rounds);
		for (int j = 0; j < rounds; j++) {
			hashData.add(new ArrayList<String>(hashRounds));
		}

		for (int i = 0; i < rounds; i++) {

			List<String> data = hashData.get(i);
			for (int j = 0; j < hashRounds; j++) {
				data.add("Objectid " + i + "" + j);
			}

		}
		return hashData;
	}

	public static double criticalVal(int hashCount, double alpha) {
		ChiSquaredDistributionImpl d = new ChiSquaredDistributionImpl(hashCount - 1);
		try {
			return d.inverseCumulativeProbability(1 - alpha);
		} catch (MathException e) {
			return Double.MIN_VALUE;
		}
	}

}
