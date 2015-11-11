package performance;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import performance.BFHashUniformity.Randoms;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Allows to count actual False Positives.
 */
public class BFHashFP {

    public static void main(String[] args) {
        testHashing();
    }

    public static void testHashing() {
        int inserts = 30000;
        double fpp = 0.01;

        testFalsePositives(inserts, fpp, Randoms.RANDWORDS);
    }

    public static void testFalsePositives(int inserts, double p, Randoms mode) {
        BloomFilter<String> dummy = new FilterBuilder()
                .expectedElements(inserts)
                .falsePositiveProbability(p)
                .buildBloomFilter();
        System.out.println(dummy);
        System.out.println("<table><tr><th>Hash function</th><th>Speed  (ms)</th><th>f during insert (%)</th><th>f final (%)</th></tr>");
        //String test = "False Positives, hashData = " + mode.getDescription() + ", inserts = " + inserts + ", size = " + m	+ ", hashes = " + k + ", Expected final FP-Rate = " + dummy.getFalsePositiveProbability(inserts) * 100;
        //System.out.println(test);


        for (HashMethod hm : HashMethod.values()) {
            testFP(hm, mode, inserts, p);
        }

        System.out.println("</table>");
    }

    public static void testFP(HashMethod hm, Randoms mode, int n, double p) {
        FilterBuilder builder = new FilterBuilder()
                .expectedElements(n)
                .falsePositiveProbability(p)
                .hashFunction(hm);

        List<byte[]> hashData = mode.generate(n, 1).get(0);
        List<byte[]> probeData = mode.generate(n * 3, 1).get(0);

        BloomFilter<String> b = builder.buildBloomFilter();
        int inserts = hashData.size();
        int fps = 0;
        Set<String> seen = new HashSet<>();
        long start = System.nanoTime();
        for (byte[] current : hashData) {
            if (b.contains(current) && !seen.contains(Randoms.fromBytes(current))) {
                fps++;
            }

            b.addRaw(current);
            seen.add(Randoms.fromBytes(current));
        }
        long end = System.nanoTime();
        double speed = (1.0 * end - start) / 1000000;

        int totalfps = 0;
        int probed = 0;
        for (byte[] current : probeData) {
            if (probed > inserts)
                break;
            if (!seen.contains(Randoms.fromBytes(current))) {
                probed++;
                if (b.contains(current)) {
                    totalfps++;
                }
            }
        }

        double fpRate = 100.0 * fps / inserts;
        double totalFpRate = 100.0 * totalfps / inserts;
        String result = "<tr><td>" + hm.toString() + "</td><td>" + speed + "</td><td>" + String.format("%1$,.3f", fpRate) + "</td><td>" + String.format("%1$,.3f", totalFpRate) + "</td></tr>";
        //String result = "HashMethod : " + hm + ", speed = " + speed + " ms, False-Positives = " + fps + ", FP-Rate = " + fpRate + ", final FP-Rate = " + totalFpRate;
        System.out.println(result);
    }
}
