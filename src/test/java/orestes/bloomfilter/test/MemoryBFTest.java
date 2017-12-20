package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.stream.IntStream;

import static orestes.bloomfilter.test.helper.Helper.createCountingFilter;
import static orestes.bloomfilter.test.helper.Helper.createFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MemoryBFTest {

    private static ArrayList<String> real;
    private static ArrayList<String> fake;
    private static ArrayList<String> hashOids;

    public static void plotHistogram(long[] histogram, String name) {
        System.out.println("Histogram for " + name + ":");
        long sum = 0;
        for (long bar : histogram) { sum += bar; }
        int index = 0;
        for (long bar : histogram) {
            double deviation = Math.abs(bar * 1.0 / sum - 1.0 / histogram.length);
            System.out.println("Index " + index++ + ": " + bar + ", Deviation from expectation: " + deviation);
        }
    }

    //@Ignore
    @Test
    public void comparison() {
        int n = 1000;
        int m = 10000;
        int k = 10;
        for (HashMethod hm : HashMethod.values()) {
            BloomFilter<String> bf = createCountingFilter(m, k, hm);
            benchmark(bf, hm.toString(), n);
        }
    }

    public static void benchmark(BloomFilter<String> bf, String configuration, int n) {
        int hashCount = 100000;

        System.out.println(configuration);
        System.out.print("hashes = " + bf.getHashes());
        System.out.print(" falsePositiveProbability = " + bf.getFalsePositiveProbability(n));
        System.out.print(" expectedElements = " + n);
        System.out.println(" size = " + bf.getSize());
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
        int hashRounds = hashCount / bf.getHashes();
        long[] observed = new long[bf.getSize()];
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

        double[] expected = new double[bf.getSize()];
        for (int i = 0; i < bf.getSize(); i++) {
            expected[i] = hashCount * 1.0 / bf.getSize();
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

        System.out.println("Hash Quality (Chi-Squared-Test): p-value = " + pValue + " , Chi-Squared-Statistic = " + chiSq);
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
    public void MD5performance() {
        int m = 100;
        int k = 1000000;
        BloomFilter<String> b = createFilter(m, k, HashMethod.MD5);
        long begin = System.nanoTime();
        long[] observed = new long[m];
        int[] hashValues = b.hash("Performance!");
        for (int i : hashValues) {
            observed[i]++;
        }
        long end = System.nanoTime();
        System.out.println("Time for calculating 1 million hashes with MD5: " + (end - begin) * 1.0 / 1000000000 + " s");
    }

    @Test
    public void SHAperformance() {
        int m = 100;
        int k = 1000000;
        BloomFilter<String> b = createFilter(m, k, HashMethod.SHA512);
        long begin = System.nanoTime();
        long[] observed = new long[m];
        int[] hashValues = b.hash("Performance!");
        for (int i : hashValues) {
            observed[i]++;
        }
        long end = System.nanoTime();
        System.out.println("Time for calculating 1 million hashes with SHA-512: " + (end - begin) * 1.0 / 1000000000 + " s");
    }


    @Test
    public void normalPerformance() {
        int inserts = 100000;
        int n = 100000;
        double p = 0.02;
        BloomFilter<String> b = createFilter(n, p, HashMethod.MD5);
        CountingBloomFilter<String> cb = createCountingFilter(n, p, HashMethod.MD5);
        System.out.println("Size of bloom filter: " + b.getSize() + ", hash functions: " + b.getHashes());
        long begin = System.nanoTime();
        for (int i = 0; i < inserts; i++) {
            String value = "String" + i;
            b.add(value);
            cb.add(value);
        }
        long end = System.nanoTime();
        System.out.println("Total time for " + inserts + " add operations in both a counting and a normal bloom filter: " + (end - begin) * 1.0 / 1000000000 + " s");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serializeBloomFilter() throws IOException, ClassNotFoundException {
        int countingBits = 3;

        // Create a flat Bloom filter
        BloomFilter<String> fbf = createFilter(1000, 0.02, HashMethod.MD5);
        // Create a counting Bloom filter
        CountingBloomFilterMemory<String> cbf = new CountingBloomFilterMemory<>(new FilterBuilder(1000, 0.2).hashFunction
            (HashMethod.MD5).countingBits(countingBits).complete());

        byte[] byteArray;
        byte[] byteArray2;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos);
             ObjectOutput out2 = new ObjectOutputStream(bos2)) {

            // Assert we can add to Bloom filter
            assertTrue(fbf.add("foo"));
            assertTrue(cbf.add("foo"));
            assertTrue(fbf.add("bar"));
            assertTrue(cbf.add("bar"));

            // Assert element is now contained
            assertTrue(fbf.contains("foo"));
            assertTrue(cbf.contains("foo"));
            assertTrue(fbf.contains("bar"));
            assertTrue(cbf.contains("bar"));

            // Assert element is contained once
            assertEquals(1, cbf.getEstimatedCount("foo"));

            // Test all possible additions
            for (int i = 2; i < 1 << countingBits; i++) {
                assertFalse(cbf.add("foo"));
                assertEquals(i, cbf.getEstimatedCount("foo"));
            }

            // Test an overflow
            assertFalse(cbf.add("foo"));
            assertEquals((1 << countingBits) - 1, cbf.getEstimatedCount("foo"));

            // Test element is not contained
            assertFalse(fbf.contains("ksm"));
            assertFalse(cbf.contains("ksm"));

            // Test all possible removals
            for (int i = (1 << countingBits) - 2; i >= 0; i--) {
                assertEquals(i == 0, cbf.remove("foo"));
                assertEquals(i, cbf.getEstimatedCount("foo"));
            }

            // And reinsert it for later
            assertTrue(cbf.add("foo"));

            out.writeObject(fbf);
            out2.writeObject(cbf);

            byteArray = bos.toByteArray();
            byteArray2 = bos2.toByteArray();
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        ObjectInput in = new ObjectInputStream(bis);
        Object object = in.readObject();

        ByteArrayInputStream bis2 = new ByteArrayInputStream(byteArray2);
        ObjectInput in2 = new ObjectInputStream(bis2);
        Object object2 = in2.readObject();

        bis.close();
        bis2.close();
        in.close();
        in2.close();

        assertTrue(object instanceof BloomFilter);
        assertTrue(object2 instanceof CountingBloomFilter);

        BloomFilterMemory<String> bloom = (BloomFilterMemory<String>) object;
        CountingBloomFilterMemory<String> counting = (CountingBloomFilterMemory<String>) object2;

        assertTrue(bloom.contains("foo"));
        assertTrue(counting.contains("foo"));
        assertTrue(bloom.contains("bar"));
        assertTrue(counting.contains("bar"));
        IntStream.range(0,100).forEach(i -> counting.add("same object"));
    }
}
