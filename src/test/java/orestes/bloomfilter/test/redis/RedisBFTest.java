package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import orestes.bloomfilter.test.helper.Helper;
import redis.clients.jedis.Protocol;

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

import static orestes.bloomfilter.test.helper.Helper.*;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class RedisBFTest {

    private enum FilterTypes {
        NORMAL, COUNTING, POOL_CONFIG, SENTINEL_CONFIG
    }

    private final FilterTypes filterTypes;

    @Parameterized.Parameters(name = "Redis Bloom Filter test with {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {
                {"normal", FilterTypes.NORMAL},
                {"pool_config", FilterTypes.POOL_CONFIG},
                {"sentinel_config", FilterTypes.SENTINEL_CONFIG},
                {"counting", FilterTypes.COUNTING}
        };
        return Arrays.asList(data);
    }

    public RedisBFTest(String type, FilterTypes filterTypes) {
        this.filterTypes = filterTypes;
    }

    private BloomFilter<String> createFilter(String name, int n, double p, boolean overwrite) {
        return createFilter(name, n, p, overwrite, Protocol.DEFAULT_DATABASE);
    }

    private BloomFilter<String> createFilter(String name, int n, double p, boolean overwrite, int database) {
        if(filterTypes == FilterTypes.COUNTING)
            return createCountingRedisFilter(name, n, p, HashMethod.MD5, overwrite, database);
        else if (filterTypes == FilterTypes.NORMAL)
            return createRedisFilter(name, n, p, HashMethod.MD5, overwrite, database);
        else if (filterTypes == FilterTypes.POOL_CONFIG)
            return createRedisPoolFilter(name, n, p, HashMethod.MD5, overwrite, database);
        else if (filterTypes == FilterTypes.SENTINEL_CONFIG)
            return createRedisSentinelFilter(name, n, p, HashMethod.MD5, overwrite, database);
        else
            throw new IllegalArgumentException();
    }

    private BloomFilter<String> createSlaveFilter(String name, int n, double p, boolean overwrite) {
        // Only the normal Read Filter needs to know about slaves.
        if (filterTypes == FilterTypes.NORMAL)
            return createRedisFilterWithReadSlave(name, n, p, HashMethod.MD5, overwrite, host, slavePort);
        else return createFilter(name, n, p, overwrite);

    }

    private void cleanupRedis() {
        if (filterTypes == FilterTypes.SENTINEL_CONFIG) {
            cleanupRedisSentinel();
        } else {
            Helper.cleanupRedis();
        }
    }


    @Test
    public void testSlaveReads() throws Exception{
        int n = 1000;
        double p = 0.01;

        BloomFilter<String> filter = createSlaveFilter("slaves", n, p, true);

        List<String> items = IntStream.range(0, 100).mapToObj(i -> "obj" + String.valueOf(i)).collect(Collectors.toList());
        items.forEach(filter::add);

        Thread.sleep(1000);
        
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
        BloomFilter<String> first = createFilter(name, n, p, true);
        first.add(testString);
        System.out.println(first.asString());

        BloomFilter<String> loaded = createFilter(name, n, p, true);
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
        BloomFilter<String> first = createFilter(name, n, p, true);
        first.add(testString);
        System.out.println(first.asString());

        BloomFilter<String> loaded = createFilter(name, n, p, false);

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
        BloomFilter<String> first = createFilter(name, n, p, true);

        first.add(testString);

        first.remove();

        if (filterTypes == FilterTypes.SENTINEL_CONFIG) {
            assertFalse(getSentinelJedis().getResource().exists(name));
        } else {
            assertFalse(getJedis().exists(name));
        }
    }


    @Test
    public void bulkContains() {
        int n = 1000;
        double p = 0.01;

        String name = "loadExistingTest";
        String testString = "simpletest";

        cleanupRedis();
        BloomFilter<String> first = createFilter(name, n, p, true);

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

    @Test
    public void testAsNormalFilter() {
        BloomFilter<String> first = createFilter("I_m_in_Redis", 10_000, 0.01, true);
        first.add("42");
        BloomFilter<String> second;
        if(filterTypes == FilterTypes.COUNTING)
            second = ((CountingBloomFilterRedis<String>) first).toMemoryFilter();
        else
            second = ((BloomFilterRedis<String>) first).toMemoryFilter();
        assertTrue(second.contains("42"));
    }


    public static void concurrentBenchmark(List<BloomFilter<String>> bfs, int opsPerThread) {
        ExecutorService pool = Executors.newFixedThreadPool(bfs.size());
        List<Runnable> threads = new ArrayList<>(bfs.size());
        List<String> items = new ArrayList<>(opsPerThread);
        for (int i = 0; i < opsPerThread; i++) {
            items.add(String.valueOf(i));
        }
        for (BloomFilter<String> bf : bfs) {
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

    @Test
    public void testPool() throws Exception {
        BloomFilter<String> bf = createFilter("pooltest", 10_000, 0.01, true);
        RedisPool pool = bf.config().pool();

        FilterBuilder clonedConfig = bf.config().clone().name("pooltest-cloned");
        BloomFilter<String> filter = filterTypes == FilterTypes.COUNTING ? clonedConfig.buildCountingBloomFilter() : clonedConfig.buildBloomFilter();
        filter.add("filter");
        bf.add("bf");

        assertTrue(filter.contains("filter"));
        assertTrue(bf.contains("bf"));
        assertFalse(filter.contains("bf"));
        assertSame(pool, filter.config().pool());
    }

    
    @Test
    public void testDatabase() throws Exception {
        String filterName = "dbtest";
        BloomFilter<String> bfDb1 = createFilter(filterName, 10_000, 0.01, true);
        assertEquals(filterName, bfDb1.config().name());
        bfDb1.add("element1");
        assertTrue(bfDb1.contains("element1"));
        assertFalse(bfDb1.contains("element2"));
        
        BloomFilter<String> bfDb2 = createFilter(filterName, 10_000, 0.01, true, 1);
        assertEquals(filterName, bfDb2.config().name());
        assertFalse(bfDb2.contains("element1"));
        bfDb2.add("element2");
        assertTrue(bfDb2.contains("element2"));
        assertFalse(bfDb1.contains("element2"));
    }

}
