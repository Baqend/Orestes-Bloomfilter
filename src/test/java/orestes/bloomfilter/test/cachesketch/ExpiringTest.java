package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import orestes.bloomfilter.test.cachesketch.DelayGenerator.DelayNamePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.*;
import static orestes.bloomfilter.test.cachesketch.ExpiringTestHelpers.*;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ExpiringTest {
    private static final String TYPE_MEMORY_ONLY = "in-memory";
    private static final String TYPE_REDIS_MEMORY = "with Redis counts and in-memory queue";
    private static final String TYPE_REDIS_ONLY = "with Redis counts and Redis queue";
    private static final int NUMBER_OF_ELEMENTS = 100;
    private final String type;
    private ExpiringBloomFilter<String> filter;

    @Parameters(name = "Expiring Bloom Filter test {0}")
    public static Collection<Object[]> data() {
        Object[][] data = {
            {TYPE_MEMORY_ONLY},
            {TYPE_REDIS_MEMORY},
            {TYPE_REDIS_ONLY},
        };

        return Arrays.asList(data);
    }

    @After
    public void afterTest() {
        if (filter != null) {
            filter.remove();
        }
    }

    public ExpiringTest(String type) {
        this.type = type;
    }

    private void createFilter(FilterBuilder b) {
        b.overwriteIfExists(true);

        switch (type) {
            case TYPE_MEMORY_ONLY:
                filter = new ExpiringBloomFilterMemory<>(b);
                break;
            case TYPE_REDIS_MEMORY:
                filter = new ExpiringBloomFilterRedis<>(b);
                break;
            case TYPE_REDIS_ONLY:
                filter = new ExpiringBloomFilterPureRedis(b);
                break;
        }

        filter.clear();
        assertTrue("Bloom filter should be empty before", filter.isEmpty());
        assertEquals("Bloom filter's bits should have zero length", 0, filter.getBitSet().length());
        assertEquals("Bloom filter's bit cardinality should be zero", 0, filter.getBitSet().cardinality());
    }

    @Before
    public void clear() {
        if (filter != null) {
            filter.clear();
        }
    }

    @Test
    public void addAndLetExpire() {
        readAndLetExpire(true);
    }

    @Test
    public void addAndLetExpireWithoutWrite() {
        readAndLetExpire(false);
    }

    @Test
    public void testAddMultipleTimes() {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        boolean invalidation = filter.reportWrite("1");
        filter.reportRead("1", 100, MILLISECONDS);
        filter.reportRead("1", 800, MILLISECONDS);
        filter.reportRead("1", 1500, MILLISECONDS);
        filter.reportRead("1", 20, MILLISECONDS);

        Long remainingTTL = filter.getRemainingTTL("1", MILLISECONDS);

        assertFalse(invalidation);
        assertFalse(filter.contains("1"));
        assertTrue(remainingTTL <= 1500);
        assertTrue(remainingTTL > 1400);
    }


    @Test
    public void testExpiration() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, MILLISECONDS);
        filter.reportRead("1", 100, MILLISECONDS);

        long ttl1 = filter.reportWrite("1", MILLISECONDS);
        assertRemainingTTL(70, 100, ttl1);
        assertTrue(filter.contains("1"));
        assertEquals(1, Math.round(filter.getEstimatedPopulation()));

        Thread.sleep(30);

        long ttl2 = filter.getRemainingTTL("1", MILLISECONDS);
        assertRemainingTTL(15, 70, ttl2);

        Thread.sleep(150);

        Long ttl3 = filter.getRemainingTTL("1", MILLISECONDS);
        assertNull(ttl3);
        assertFalse("Element (1) should not be contained in Bloom filter", filter.contains("1"));
    }

    @Test
    public void testGracePeriod() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05).gracePeriod(2000);
        createFilter(b);

        // Test state after read
        filter.reportRead("1", 500, MILLISECONDS);
        filter.reportRead("1", 700, MILLISECONDS);
        assertTrue(filter.isKnown("1"));
        assertTrue(filter.isCached("1"));

        // Write after TTL is expired
        Thread.sleep(1000);
        Long ttl1 = filter.reportWrite("1", MILLISECONDS);
        assertNull(ttl1);
        assertFalse(filter.contains("1"));
        assertFalse(filter.isCached("1"));
        assertTrue(filter.isKnown("1"));

        // Cleanup TTLs before grace period expires
        filter.cleanupTTLs();
        assertFalse(filter.contains("1"));
        assertTrue(filter.isKnown("1"));
        assertFalse(filter.isCached("1"));

        // Cleanup TTLs after grace period expired
        Thread.sleep(2000);
        filter.cleanupTTLs();
        assertFalse(filter.contains("1"));
        assertFalse(filter.isKnown("1"));
        assertFalse(filter.isCached("1"));

        assertEquals(0, Math.round(filter.getEstimatedPopulation()));
    }

    @Test
    public void testAutoCleanupPeriod() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05)
                .gracePeriod(2000)
                .cleanupInterval(1000);
        createFilter(b);

        // Test state after read
        filter.reportRead("1", 500, MILLISECONDS);
        filter.reportRead("1", 700, MILLISECONDS);
        assertTrue(filter.isKnown("1"));
        assertTrue(filter.isCached("1"));

        Thread.sleep(1000);

        // Write after TTL is expired
        Thread.sleep(1000);
        Long ttl1 = filter.reportWrite("1", MILLISECONDS);
        assertNull(ttl1);
        assertFalse(filter.contains("1"));
        assertFalse(filter.isCached("1"));
        assertTrue(filter.isKnown("1"));

        // Wait for auto cleanup of TTLs
        Thread.sleep(2000);
        assertFalse(filter.contains("1"));
        assertFalse(filter.isKnown("1"));
        assertFalse(filter.isCached("1"));

        assertEquals(0, Math.round(filter.getEstimatedPopulation()));
    }

    @Test
    public void exceedCapacity() {
        FilterBuilder b = new FilterBuilder(100, 0.05).overwriteIfExists(true);
        createFilter(b);

        IntStream.range(0, 200).forEach(i -> {
            String elem = String.valueOf(i);
            filter.reportRead(elem, 1000, SECONDS);
            filter.reportWrite(elem);
            assertTrue(filter.contains(elem));
            //System.out.println(filter.getEstimatedPopulation() + ":" + filter.getEstimatedFalsePositiveProbability());
        });
        //System.out.println(filter.getFalsePositiveProbability(200));

        //Materialized size roughly equal to estimated size
        assertLessThan(10, Math.abs(filter.getEstimatedPopulation() - 200));
        //fpp exceeded
        assertGreaterThan(0.05, filter.getEstimatedFalsePositiveProbability());
        //Less then 10% difference between estimated and precise fpp
        double diff = Math.abs(1 - (filter.getEstimatedFalsePositiveProbability() / filter.getFalsePositiveProbability(200)));
        assertLessThan(0.1, diff);
    }

    @Test
    public void testClone() {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, MILLISECONDS);
        filter.reportRead("2", 50, MILLISECONDS);
        filter.reportWrite("1");
        BloomFilter<String> bf = filter.getClonedBloomFilter();
        filter.reportWrite("2");
        assertTrue(bf.contains("1"));
        assertFalse(bf.contains("2"));
    }

    @Test
    public void testReportMultipleWrites() {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, MILLISECONDS);
        filter.reportRead("2", 50, MILLISECONDS);
        filter.reportWrites(Arrays.asList("1","2"));
        assertTrue(filter.contains("1"));
        assertTrue(filter.contains("2"));
    }

    @Test
    public void testClear() {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, MILLISECONDS);
        filter.reportRead("2", 50, MILLISECONDS);
        filter.reportWrite("1");
        assertTrue(filter.isCached("1"));
        assertTrue(filter.isCached("2"));
        assertTrue(filter.contains("1"));
        assertFalse(filter.contains("2"));
        assertFalse(filter.isEmpty());

        filter.clear();
        assertFalse(filter.contains("1"));
        assertFalse(filter.contains("2"));
        assertTrue(filter.isEmpty());
        assertNull(filter.getRemainingTTL("1", MILLISECONDS));
        assertNull(filter.getRemainingTTL("2", MILLISECONDS));
    }

    @Test
    public void testGetTtls() {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(i);
            keys.add(key);
            filter.reportRead(key, 50, SECONDS);
        }

        List<Long> ttls = filter.getRemainingTTLs(keys, SECONDS);

        for (Long ttl : ttls) {
            assertTrue(ttl >= 49);
        }
    }

    @Test
    public void testIsKnownList() throws InterruptedException {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        b.gracePeriod(3_000);
        createFilter(b);

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        List<String> keys = new ArrayList<>();
        keys.add(key1);
        filter.reportRead(key1, 1, SECONDS);
        keys.add(key2);
        filter.reportRead(key2, 2, SECONDS);
        keys.add(key3);
        filter.reportRead(key3, 3, SECONDS);

        // initially: all cached, all known
        assertTrue(filter.isCached(key1));
        assertTrue(filter.isCached(key2));
        assertTrue(filter.isCached(key3));
        assertEquals(Arrays.asList(true, true, true), filter.isKnown(keys));
        assertTrue(filter.isKnown(key1));
        assertTrue(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_100);

        // after 1100 ms: key1 expired, all known
        assertFalse(filter.isCached(key1));
        assertTrue(filter.isCached(key2));
        assertTrue(filter.isCached(key3));
        assertEquals(Arrays.asList(true, true, true), filter.isKnown(keys));
        assertTrue(filter.isKnown(key1));
        assertTrue(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_000);

        // after 2100 ms: key1/key2 expired, all known
        assertFalse(filter.isCached(key1));
        assertFalse(filter.isCached(key2));
        assertTrue(filter.isCached(key3));
        assertEquals(Arrays.asList(true, true, true), filter.isKnown(keys));
        assertTrue(filter.isKnown(key1));
        assertTrue(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_000);

        // after 3100 ms: all expired, all known
        assertFalse(filter.isCached(key1));
        assertFalse(filter.isCached(key2));
        assertFalse(filter.isCached(key3));
        assertEquals(Arrays.asList(true, true, true), filter.isKnown(keys));
        assertTrue(filter.isKnown(key1));
        assertTrue(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_000);

        // after 4100 ms: all expired, key1 unknown
        assertFalse(filter.isCached(key1));
        assertFalse(filter.isCached(key2));
        assertFalse(filter.isCached(key3));
        assertEquals(Arrays.asList(false, true, true), filter.isKnown(keys));
        assertFalse(filter.isKnown(key1));
        assertTrue(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_000);

        // after 5100 ms: all expired, key1/key2 unknown
        assertFalse(filter.isCached(key1));
        assertFalse(filter.isCached(key2));
        assertFalse(filter.isCached(key3));
        assertEquals(Arrays.asList(false, false, true), filter.isKnown(keys));
        assertFalse(filter.isKnown(key1));
        assertFalse(filter.isKnown(key2));
        assertTrue(filter.isKnown(key3));
        Thread.sleep(1_000);

        // after 6100 ms: all expired, all unknown
        assertFalse(filter.isCached(key1));
        assertFalse(filter.isCached(key2));
        assertFalse(filter.isCached(key3));
        assertEquals(Arrays.asList(false, false, false), filter.isKnown(keys));
        assertFalse(filter.isKnown(key1));
        assertFalse(filter.isKnown(key2));
        assertFalse(filter.isKnown(key3));
    }

    @Test
    public void testGetTimeToLiveMap() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        filter.reportRead("Foo", 2, SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 4, SECONDS);
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        filter.reportRead("Baz", 3, SECONDS);
        filter.reportWrite("Baz");
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));

        TimeMap<String> map = filter.getTimeToLiveMap();
        assertEquals(3, map.size());
        assertTrue(map.containsKey("Foo"));
        assertTimeBetween(1, 2, SECONDS, map, "Foo");
        assertTrue(map.containsKey("Bar"));
        assertTimeBetween(3, 4, SECONDS, map, "Bar");
        assertTrue(map.containsKey("Baz"));
        assertTimeBetween(2, 3, SECONDS, map, "Baz");
    }

    @Test
    public void testSetTimeToLiveMap() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        assertFalse(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.isCached("Baz"));
        assertFalse(filter.contains("Baz"));

        TimeMap<String> map = new TimeMap<>();
        map.putRemaining("Foo", 2L, SECONDS);
        map.putRemaining("Bar", 4L, SECONDS);
        map.putRemaining("Baz", 3L, SECONDS);

        filter.setTimeToLiveMap(map);

        assertTrue(filter.isCached("Foo"));
        assertTrue(filter.getRemainingTTL("Foo", MILLISECONDS) > 1000L);
        assertFalse(filter.contains("Foo"));

        assertTrue(filter.isCached("Bar"));
        assertTrue(filter.getRemainingTTL("Bar", MILLISECONDS) > 3000L);
        assertFalse(filter.contains("Bar"));

        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.getRemainingTTL("Baz", MILLISECONDS) > 2000L);
        assertFalse(filter.contains("Baz"));
    }

    @Test
    public void testGetExpirationMap() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        filter.reportRead("Foo", 2, SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 4, SECONDS);
        filter.reportWrite("Bar");
        assertTrue(filter.isCached("Bar"));
        assertTrue(filter.contains("Bar"));
        filter.reportRead("Baz", 3, SECONDS);
        filter.reportWrite("Baz");
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));

        TimeMap<String> map = filter.getExpirationMap();
        assertEquals(2, map.size());
        assertFalse(map.containsKey("Foo"));
        assertTrue(map.containsKey("Bar"));
        assertTimeBetween(3, 4, SECONDS, map, "Bar");
        assertTrue(map.containsKey("Baz"));
        assertTimeBetween(2, 3, SECONDS, map, "Baz");
    }

    @Test
    public void testSetExpirationMap() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        assertFalse(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.isCached("Baz"));
        assertFalse(filter.contains("Baz"));

        TimeMap<String> map = new TimeMap<>();
        map.putRemaining("Foo", 2L, SECONDS);
        map.putRemaining("Bar", 4L, SECONDS);
        map.putRemaining("Baz", 3L, SECONDS);

        filter.setExpirationMap(map);

        TimeMap<String> actualMap = filter.getExpirationMap();
        assertEquals(map, actualMap);
    }

    @Test
    public void testMigrateFromInMemoryExpiringBloomFilter() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);

        // Create the filter to migrate from
        ExpiringBloomFilterMemory<String> inMemory = new ExpiringBloomFilterMemory<>(b);
        inMemory.reportRead("Foo", 50, SECONDS);
        assertTrue(inMemory.isCached("Foo"));
        assertFalse(inMemory.contains("Foo"));
        inMemory.reportRead("Bar", 40, SECONDS);
        assertTrue(inMemory.isCached("Bar"));
        assertFalse(inMemory.contains("Bar"));
        inMemory.reportRead("Baz", 30, SECONDS);
        inMemory.reportWrite("Baz");
        assertTrue(inMemory.isCached("Baz"));
        assertTrue(inMemory.contains("Baz"));

        // Check state before migration
        createFilter(b);
        assertFalse(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.isCached("Baz"));
        assertFalse(filter.contains("Baz"));

        // Check state after migration
        inMemory.migrateTo(filter);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertRemainingTTL(30, 50, filter.getRemainingTTL("Foo", SECONDS));
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertRemainingTTL(20, 40, filter.getRemainingTTL("Bar", SECONDS));
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));
        assertRemainingTTL(10, 30, filter.getRemainingTTL("Baz", SECONDS));

        // Cleanup in-memory BF
        inMemory.clear();
    }

    @Test
    public void testMigrateToInMemoryExpiringBloomFilter() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);

        // Create the filter to migrate from
        createFilter(b);
        filter.reportRead("Foo", 3, SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 2, SECONDS);
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        filter.reportRead("Baz", 1, SECONDS);
        filter.reportWrite("Baz");
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));

        // Check state before migration
        ExpiringBloomFilterMemory<String> inMemory = new ExpiringBloomFilterMemory<>(b);
        assertFalse(inMemory.isCached("Foo"));
        assertFalse(inMemory.contains("Foo"));
        assertFalse(inMemory.isCached("Bar"));
        assertFalse(inMemory.contains("Bar"));
        assertFalse(inMemory.isCached("Baz"));
        assertFalse(inMemory.contains("Baz"));

        // Check state after migration
        filter.migrateTo(inMemory);
        assertTrue(inMemory.isCached("Foo"));
        assertFalse(inMemory.contains("Foo"));
        assertRemainingTTL(2, 2, inMemory.getRemainingTTL("Foo", SECONDS));
        assertTrue(inMemory.isCached("Bar"));
        assertFalse(inMemory.contains("Bar"));
        assertRemainingTTL(1, 2, inMemory.getRemainingTTL("Bar", SECONDS));
        assertTrue(inMemory.isCached("Baz"));
        assertTrue(inMemory.contains("Baz"));
        assertRemainingTTL(0, 1, inMemory.getRemainingTTL("Baz", SECONDS));

        // Ensure everything expires as suspected
        Thread.sleep(3000);
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.contains("Baz"));
        assertFalse(inMemory.contains("Foo"));
        assertFalse(inMemory.contains("Bar"));
        assertFalse(inMemory.contains("Baz"));

        // Cleanup in-memory BF
        inMemory.clear();
    }

    @Test
    public void testSoftClear() {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        filter.reportRead("Foo", 70, SECONDS);
        filter.reportWrite("Foo");
        assertTrue(filter.getExpirationMap().containsKey("Foo"));
        assertTrue(filter.contains("Foo"));

        filter.softClear();
        assertFalse(filter.contains("Foo"));
        assertEquals(1, filter.getRemainingTTL("Foo", MINUTES).longValue());
        assertTrue(filter.isKnown("Foo"));
        assertFalse(filter.getExpirationMap().containsKey("Foo"));

        filter.reportWrite("Foo");
        assertTrue(filter.getExpirationMap().containsKey("Foo"));
        assertTrue(filter.contains("Foo"));
    }

    private void readAndLetExpire(boolean reportWrite) {
        // Create Bloom filter
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        b.redisConnections(NUMBER_OF_ELEMENTS);
        createFilter(b);

        // Assert we get no false positives
        for (int i = 0; i < NUMBER_OF_ELEMENTS; i++) {
            String item = String.valueOf(i);
            assertFalse(filter.contains(item));
            filter.add(item);
        }
        filter.clear();

        AtomicInteger count = new AtomicInteger(0);

        DelayGenerator delayGenerator = new DelayGenerator(NUMBER_OF_ELEMENTS);
        ExecutorService threads = Executors.newFixedThreadPool(100);
        List<CompletableFuture> futures = new LinkedList<>();
        for (DelayNamePair pair : delayGenerator) {
            int delay = pair.getDelay();
            String item = pair.getItem();
            futures.add(CompletableFuture.runAsync(() -> {
                // Check Bloom filter state before read
                assertFalse(filter.isCached(item));
                assertFalse(filter.contains(item));
                assertNull(filter.getRemainingTTL(item, MILLISECONDS));

                filter.reportRead(item, delay, MILLISECONDS);

                // Check Bloom filter state after read
                assertTrue(filter.isCached(item));
                assertFalse(filter.contains(item));
                assertBetween(0, delay, filter.getRemainingTTL(item, MILLISECONDS));

                if (reportWrite) {
                    assertTrue(filter.reportWrite(item));

                    // Check Bloom filter state after write
                    assertTrue(filter.contains(item));
                    assertFalse(filter.isEmpty());
                }
                assertTrue(filter.isCached(item));

                // Wait for the delay to pass
                try {
                    Thread.sleep(delay + 2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Check Bloom filter state after expire
                Long remaining = filter.getRemainingTTL(item, MILLISECONDS);
                if (filter.contains(item)) {
                    fail("Element (" + item + ") still in Bloom filter. remaining TTL: " + remaining);
                }
                assertFalse(filter.isCached(item));
                assertFalse(filter.contains(item));
                assertNull(remaining);
                count.incrementAndGet();
            }, threads));
        }

        // Wait for tasks to complete
        futures.forEach(CompletableFuture::join);

        // Ensure Bloom filter is empty
        assertEquals(NUMBER_OF_ELEMENTS, count.get());
        for (int i = 0; i < NUMBER_OF_ELEMENTS; i++) {
            String item = String.valueOf(i);
            assertFalse(filter.contains(item));
        }

        assertEmpty(filter);
    }
}
