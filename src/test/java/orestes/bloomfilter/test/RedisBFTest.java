package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.test.helper.Helper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static orestes.bloomfilter.test.helper.Helper.*;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class RedisBFTest {

    private final boolean counts;

    @Parameterized.Parameters(name = "Redis Bloom Filter test with {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {
                {"normal", false},
                {"counting", true}
        };
        return Arrays.asList(data);
    }

    public RedisBFTest(String type, boolean counts) {
        this.counts = counts;
    }

    private BloomFilter<String> createFilter(String name, int n, double p) {
        if(counts)
            return createCountingRedisFilter(name, n, p, HashMethod.MD5);
        else
            return createRedisFilter(name, n, p, HashMethod.MD5);
    }


    @Test
    public void testSlaveReads() throws Exception{
        int m = 1000, k = 10;
        FilterBuilder fb = new FilterBuilder(m,k)
                .name("slavetest")
                .redisBacked(true)
                .addReadSlave(Helper.host, Helper.port);
                //.addReadSlave(Helper.host, Helper.port +1);

        BloomFilter<String> filter = counts ? fb.buildCountingBloomFilter() : fb.buildBloomFilter();

        List<String> items = IntStream.range(0, 100).mapToObj(i -> "obj" + String.valueOf(i)).collect(Collectors.toList());
        items.forEach(filter::add);

        //On localhost, there is no perceivable replication lag
        //Thread.sleep(10);

        assertTrue(filter.containsAll(items));
        items.forEach(i -> assertTrue(filter.contains(i)));

        filter.remove();
    }


    @Test
    public void overwriteExistingFilter() {
        int n = 1000;
        double p = 0.01;

        String name = "loadExistingTest";
        String testString = "simpletest";
        String testString2 = "simpletest2";

        cleanupRedis();
        BloomFilter<String> first = createFilter(name, n, p);
        first.add(testString);
        System.out.println(first.asString());

        BloomFilter<String> loaded = createFilter(name, n, p);
        System.out.println(loaded.asString());
        assertFalse(loaded.contains(testString));
        assertTrue(loaded.getExpectedElements() == n);
        assertEquals(first.getSize(), loaded.getSize());
        assertEquals(first.getHashes(), loaded.getHashes());
        assertEquals(0, Math.round(first.getEstimatedPopulation()));

        loaded.add(testString2);

        assertTrue(first.contains(testString2));

        cleanupRedis();
    }

    @Test
    public void loadExistingFilter() {
        int n = 1000;
        double p = 0.01;

        String name = "loadExistingTest";
        String testString = "simpletest";
        String testString2 = "simpletest2";

        cleanupRedis();
        BloomFilter<String> first = createFilter(name, n, p);
        first.add(testString);
        System.out.println(first.asString());

        BloomFilter<String> loaded;
        if(counts)
            loaded = new FilterBuilder(n, p).name(name).redisBacked(true).buildCountingBloomFilter();
        else
            loaded = new FilterBuilder(n, p).name(name).redisBacked(true).buildBloomFilter();

        System.out.println(loaded.asString());
        assertTrue(loaded.contains(testString));

        loaded.add(testString2);

        assertTrue(first.contains(testString2));

        cleanupRedis();
    }

    @Test
    public void removeExistingFilter() {
        int n = 1000;
        double p = 0.01;

        String name = "loadExistingTest";
        String testString = "simpletest";

        cleanupRedis();
        BloomFilter<String> first = createFilter(name, n, p);

        first.add(testString);

        first.remove();

        assert(!getJedis().exists(name));

    }


    @Test
    public void bulkContains() {
        int n = 1000;
        double p = 0.01;

        String name = "loadExistingTest";
        String testString = "simpletest";

        cleanupRedis();
        BloomFilter<String> first = createFilter(name, n, p);

        first.add(testString);

        List<String> samples = new ArrayList<>();
        samples.add("one");
        samples.add("two");
        samples.add("three");
        samples.add("four");

        first.addAll(samples);

        samples.add("five");
        samples.add("six");
        samples.add(testString);

        List<Boolean> exists = first.contains(samples);

        assertTrue(exists.get(0));       // "one"
        assertTrue(exists.get(1));       // "two"
        assertTrue(exists.get(2));       // "three"
        assertTrue(exists.get(3));       // "four"
        assertTrue(!exists.get(4));       // "five"
        assertTrue(!exists.get(5));       // "six"
        assertTrue(exists.get(6));       // "simpleTest"
        ArrayList<String> testPositive = new ArrayList<>();
        testPositive.add("one");
        testPositive.add("two");
        testPositive.add("three");
        testPositive.add("four");
        ArrayList<String> testNegative = new ArrayList<>();
        testNegative.add("five");
        testNegative.add("six");
        assertTrue(first.containsAll(testPositive));
        assertFalse(first.containsAll(testNegative));


    }


    public static void concurrentBenchmark(List<BloomFilter<String>> bfs, final int opsPerThread) {
        ExecutorService pool = Executors.newFixedThreadPool(bfs.size());
        List<Runnable> threads = new ArrayList<>(bfs.size());
        final List<String> items = new ArrayList<>(opsPerThread);
        for (int i = 0; i < opsPerThread; i++) {
            items.add(String.valueOf(i));
        }
        for (final BloomFilter<String> bf : bfs) {
            threads.add(new Runnable() {

                @Override
                public void run() {
                    for (int i = 0; i < opsPerThread; i++) {
                        bf.add(String.valueOf(i));
                    }
                    for (int i = 0; i < opsPerThread; i++) {
                        bf.contains(String.valueOf(i));
                    }
                    bf.addAll(items);
                }
            });
        }
        long start = System.nanoTime();
        for (Runnable r : threads) {
            pool.execute(r);
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            long end = System.nanoTime();
            System.out.println("Concurrent Benchmark, " + opsPerThread + " ops * " + bfs.size() + " threads = " + opsPerThread * bfs.size() + " total ops: " + ((double) (end - start)) / 1000000 + " ms");
        } catch (InterruptedException e) {
            //...
        }
    }

}