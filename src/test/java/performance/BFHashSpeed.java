package performance;

import orestes.bloomfilter.HashProvider.HashFunction;
import orestes.bloomfilter.HashProvider.HashMethod;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import performance.BFHashUniformity.Randoms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BFHashSpeed {
	private static ArrayList<ArrayList<String>> hashData;

	public static void main(String[] args) {
		// Calculated Hash Values
		int hashesPerRound = 100000;
		// Buckets
		int m = 1000;
		// Values per Hash functions
		int k = 10;
		// Successive runs
		int rounds = 20;
        //Random Values
        Randoms mode = Randoms.INTS;

		speedTests(hashesPerRound, rounds,  m, k, mode);
	}

	public static void speedTests(int hashesPerRound, int rounds, int m, int k, Randoms mode) {
		String mathematica = "";

		List<List<byte[]>> hashData = mode.generate(hashesPerRound, rounds);

        for (HashMethod hm : HashMethod.values()) {
            mathematica += testSpeed(hm, hashData, hashesPerRound, rounds,  m, k) + ",";
        }
        mathematica = mathematica.substring(0, mathematica.length() - 1);


		String speedVar = "speed" + hashesPerRound + "h" + m + "size" + k + "hashes";
		String config = "hashes = " + hashesPerRound + ", size = " + m + ", hashes = " + k + ", rounds = " + hashesPerRound;
		System.out.println(speedVar + " = { " + mathematica + "};");
		System.out.println("showSpeed[" + speedVar + ", \"Speed (ms)\\expectedElements" + config + "\"]");
	}

	public static String testSpeed(HashMethod hm, List<List<byte[]>> hashData, int hashesPerRound, int m, int k, int rounds) {
		DescriptiveStatistics speed = new DescriptiveStatistics();

        HashFunction hf = hm.getHashFunction();
        int hashRounds = hashesPerRound / k;

		Random r = new Random();
		for (int i = 0; i < rounds; i++) {
			List<byte[]> data = hashData.get(i);

			long start_hash = System.nanoTime();
			for (int i1 = 0; i1 < hashRounds; i1++) {
				int[] hashes = hf.hash(data.get(i1), m, k);
			}
			long end_hash = System.nanoTime();

			speed.addValue((1.0 * end_hash - start_hash) / 1000000);
		}

		System.out.println("HashMethod : " + hm + ", " + " hashes = " + hashesPerRound + ", size = " + m + ", hashes = "
				+ k + ", rounds = " + rounds);
		//System.out.println(speed);

		return Arrays.toString(speed.getValues()).replace("[", "{").replace("]", "}");
	}
	
}
