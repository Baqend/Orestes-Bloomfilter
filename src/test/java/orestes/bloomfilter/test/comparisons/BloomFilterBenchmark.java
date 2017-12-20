package orestes.bloomfilter.test.comparisons;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.BloomFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A (very) simple benchmark to evaluate the performance of the Bloom filter class.
 *
 * @author Magnus Skjegstad
 */
public class BloomFilterBenchmark {
    static int elementCount = 50000; // Number of elements to test

    public static void printStat(long start, long end) {
        double diff = (end - start) / 1000.0;
        diff = (double)Math.round(diff * 10000) / 10000;
        double speed = (elementCount / diff);
        speed = (double)Math.round(speed * 10000) / 10000;
        System.out.println(diff + "s, " + speed + " elements/s");
    }

    public static void main(String[] argv) {
        

        Random r = new Random();

        // Generate elements first
        List<String> existingElements = new ArrayList<>(elementCount);
        for (int i = 0; i < elementCount; i++) {
            byte[] b = new byte[200];
            r.nextBytes(b);
            existingElements.add(new String(b));
        }
        
        List<String> nonExistingElements = new ArrayList<>(elementCount);
        for (int i = 0; i < elementCount; i++) {
            byte[] b = new byte[200];
            r.nextBytes(b);
            nonExistingElements.add(new String(b));
        }

        //Mine
        BloomFilter<String> bf = new FilterBuilder(elementCount, 0.001).buildBloomFilter();
//        bf.setCryptographicHashFunction("SHA-512");
        //Counting
//        CountingBloomFilter<String> bf = new CountingBloomFilter<String>(elementCount, 0.001, 4);
//        bf.setHashFunction("SHA-512");
        //Skjegstad's
//        BloomFilterMagnus<String> bf = new BloomFilterMagnus<String>(0.001, elementCount);

        System.out.println("Testing " + elementCount + " elements");
        System.out.println("hashes is " + bf.getHashes());
        System.out.println("falsePositiveProbability is " + bf.getFalsePositiveProbability(elementCount));
        System.out.println("size is " + bf.getSize());

        // Add elements
        System.out.print("add(): ");
        long start_add = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.add(existingElements.get(i));
        }
        long end_add = System.currentTimeMillis();
        printStat(start_add, end_add);

        // Check for existing elements with contains()
        System.out.print("contains(), existing: ");
        long start_contains = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.contains(existingElements.get(i));
        }
        long end_contains = System.currentTimeMillis();
        printStat(start_contains, end_contains);

        // Check for existing elements with containsAll()
        System.out.print("containsAll(), existing: ");
        long start_containsAll = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.contains(existingElements.get(i));
        }
        long end_containsAll = System.currentTimeMillis();
        printStat(start_containsAll, end_containsAll);

        // Check for nonexisting elements with contains()
        System.out.print("contains(), nonexisting: ");
        long start_ncontains = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.contains(nonExistingElements.get(i));
        }
        long end_ncontains = System.currentTimeMillis();
        printStat(start_ncontains, end_ncontains);

        // Check for nonexisting elements with containsAll()
        System.out.print("containsAll(), nonexisting: ");
        long start_ncontainsAll = System.currentTimeMillis();
        for (int i = 0; i < elementCount; i++) {
            bf.contains(nonExistingElements.get(i));
        }
        long end_ncontainsAll = System.currentTimeMillis();
        printStat(start_ncontainsAll, end_ncontainsAll);

    }
}
