package performance;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashFunction;
import orestes.bloomfilter.HashProvider.HashMethod;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.ChiSquaredDistributionImpl;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;

import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import java.util.function.Function;

public class BFHashUniformity {
    public static long seed = 324234234;


    public static void main(String[] args) {

        //ByteBuffer bytes = ByteBuffer.wrap(bytes, off, len).order(ByteOrder.LITTLE_ENDIAN);
        //HashProvider.murmur3BB(bytes);

        testHashing();
    }

    public static void testHashing() {

        int hashesPerRound = 100_000;
        int m = 1_000;
        int k = 10;
        int rounds = 100;
        double alpha = 0.05;

        //for(Randoms mode : Randoms.values()) {
            Randoms mode = Randoms.RANDWORDS;
            StringBuilder log = new StringBuilder();
            testHashDistribution(hashesPerRound, rounds, m, k, alpha, log, mode);
            System.out.println(log.toString());
        //}
    }


    public static void testHashDistribution(int hashesPerRound, int rounds, int m, int k, double alpha,
                                            StringBuilder log, Randoms mode) {

        List<List<byte[]>> hashData = mode.generate(hashesPerRound, rounds);

        double crit = criticalVal(m, alpha);
        //System.out.println("The critical Chi-Squared-Value for alpha = " + alpha + " is " + crit);

        // Format the data for processing in Mathematica
        StringBuilder chi = new StringBuilder();
        StringBuilder pvalue = new StringBuilder();

        String methods = "{";
        for (HashMethod hm : HashMethod.values()) {
            methods += "\"" + hm.toString() + "\",";
        }
        System.out.println(methods.substring(0, methods.length() - 1) + "}");

        for (HashMethod hm : HashMethod.values()) {
            testDistribution(hm, hashesPerRound, m, k, rounds, hashData, chi, pvalue);
        }

        // Remove trailing comma
        String chiMathematica = chi.toString();
        chiMathematica = chiMathematica.substring(0, chiMathematica.length() - 1);
        String pValueMathematica = pvalue.toString();
        pValueMathematica = pValueMathematica.substring(0, pValueMathematica.length() - 1);

        String dataSource = mode.getDescription();

        String config = "hashes = " + hashesPerRound + ", size = " + m + ", hashes = " + k + ", rounds = " + rounds
                + ", hashData = " + dataSource;

        log.append("data" + hashesPerRound + "h" + m + "size" + k + "hashes" + " = { " + chiMathematica + "};\n");
        log.append("crit" + hashesPerRound + "h" + m + "size" + k + "hashes" + " = " + crit + ";\n");
        log.append("show[data" + hashesPerRound + "h" + m + "size" + k + "hashes" + ", crit" + hashesPerRound + "h" + m + "size" + k + "hashes"
                + ", \"Chi-Squared Statistic\\n" + config + "\"]\n");

        log.append("pdata" + hashesPerRound + "h" + m + "size" + k + "hashes" + " = { " + pValueMathematica + "};\n");
        log.append("pcrit" + hashesPerRound + "h" + m + "size" + k + "hashes" + " = " + alpha + ";\n");
        log.append("show[pdata" + hashesPerRound + "h" + m + "size" + k + "hashes" + ", pcrit" + hashesPerRound + "h" + m + "size" + k + "hashes"
                + ", \"p-Value\\n" + config + "\"]\n");

    }


    public static void testDistribution(HashMethod hm, int hashesPerRound, int m, int k, int rounds,
                                        List<List<byte[]>> hashData, StringBuilder chi, StringBuilder pvalue) {
        DescriptiveStatistics pValues = new DescriptiveStatistics();
        DescriptiveStatistics xs = new DescriptiveStatistics();

        HashFunction hf = hm.getHashFunction();
        int hashRounds = hashesPerRound / k;

        for (int i = 0; i < rounds; i++) {
            List<byte[]> data = hashData.get(i);
            long[] observed = new long[m];
            for (int j = 0; j < hashRounds; j++) {
                int[] hashes = hf.hash(data.get(j), m, k);
                for (int h : hashes) {
                    observed[h]++;
                }
            }
            //MemoryBFTest.plotHistogram(observed, hm.toString());

            double[] expected = new double[m];
            for (int j = 0; j < m; j++) {
                expected[j] = hashesPerRound * 1.0 / m;
            }

            ChiSquareTestImpl cs = new ChiSquareTestImpl();
            try {
                pValues.addValue(cs.chiSquareTest(expected, observed));
                xs.addValue(cs.chiSquare(expected, observed));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        String result = "HashMethod : " + hm + ", " + " hashes = " + hashesPerRound + ", size = " + m + ", hashes = "
                + k + ", rounds = " + rounds;
        //System.out.println(result);

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


    public static double criticalVal(int hashCount, double alpha) {
        ChiSquaredDistributionImpl d = new ChiSquaredDistributionImpl(hashCount - 1);
        try {
            return d.inverseCumulativeProbability(1 - alpha);
        } catch (MathException e) {
            return Double.MIN_VALUE;
        }
    }


    public static enum Randoms {
        UUIDS("UUIDs", i -> UUID.randomUUID().toString()),
        INTS("increasing integers", i -> i + ""),
        ID("incresing String IDs", i -> "object" + i),
        RANDINTS("random integers", i -> new Random(i).nextInt() + ""),
        RANDWORDS("random words", i ->
            RandomStringUtils.random(new Random(i).nextInt(100))
        ),
        BYTES("increasing bytes", i -> {
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(i);
            return fromBytes(b.array());
        });

        private final Function<Integer, String> generator;
        private String name;

        Randoms(String name, Function<Integer, String> generator) {
            this.name = name;
            this.generator = generator;
        }

        public List<List<byte[]>> generate(int hashesPerRound, int rounds) {
            return generate(hashesPerRound, rounds, generator, Randoms::toBytes);
        }

        private static <T,K> List<List<K>> generate(int hashesPerRound, int rounds, Function<Integer, T> generator, Function<T, K> converter) {
            List<List<K>> hashData = new ArrayList<>(rounds);
            for (int j = 0; j < rounds; j++) {
                hashData.add(new ArrayList<>(hashesPerRound));
            }
            for (int i = 0; i < rounds; i++) {

                List<K> data = hashData.get(i);
                Set<T> seen = new HashSet<>(hashesPerRound);
                for (int j = 0; j < hashesPerRound; j++) {
                    T newString = generator.apply(j + i * hashesPerRound);
                    if (!seen.contains(newString))
                        data.add(converter.apply(newString));
                    seen.add(newString);
                }

            }
            return hashData;
        }

        public static byte[] toBytes(String element) {
            return element.getBytes(FilterBuilder.defaultCharset());
        }

        public static String fromBytes(byte[] bytes) {
            return new String(bytes, FilterBuilder.defaultCharset());
        }

        public String getDescription() {
            return name;
        }


    }

}
