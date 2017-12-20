package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.cachesketch.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ExpiringTest {
    public static final String TYPE_MEMORY_ONLY = "in-memory";
    public static final String TYPE_REDIS_MEMORY = "with Redis counts and in-memory queue";
    public static final String TYPE_REDIS_ONLY = "with Redis counts and Redis queue";
    private final String type;
    private ExpiringBloomFilter<String> filter;

    @Parameterized.Parameters(name = "Expiring Bloom Filter test {0}")
    public static Collection<Object[]> data() throws Exception {
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

    public void createFilter(FilterBuilder b) {
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
    public void addAndLetExpire() throws Exception {
        int NUMBER_OF_ELEMENTS = 100;

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

        Random r = new Random(NUMBER_OF_ELEMENTS);
        ExecutorService threads = Executors.newFixedThreadPool(100);
        List<CompletableFuture> futures = new LinkedList<>();
        for (int i = 0; i < NUMBER_OF_ELEMENTS; i++) {
            int delay = r.nextInt(NUMBER_OF_ELEMENTS) * 10 + 1000;
            String item = String.valueOf(i);
            futures.add(CompletableFuture.runAsync(() -> {
                // Check Bloom filter state before read
                assertFalse(filter.isCached(item));
                assertFalse(filter.contains(item));
                assertNull(filter.getRemainingTTL(item, MILLISECONDS));

                filter.reportRead(item, delay, MILLISECONDS);

                // Check Bloom filter state after read
                assertTrue(filter.isCached(item));
                assertFalse(filter.contains(item));
                assertTrue(filter.getRemainingTTL(item, MILLISECONDS) >= 0);
                assertTrue(filter.getRemainingTTL(item, MILLISECONDS) <= delay);
                boolean invalidation = filter.reportWrite(item);
                assertTrue(invalidation);

                // Check Bloom filter state after write
                assertTrue(filter.isCached(item));
                assertTrue(filter.contains(item));
                assertFalse(filter.isEmpty());

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
                assertEquals(null, remaining);
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

        assertEquals(0, filter.getBitSet().length());
        assertTrue("Bloom filter should be empty after", filter.isEmpty());
        assertEquals(0, filter.getBitSet().cardinality());
    }

    @Test
    public void testAddMultipleTimes() throws Exception {
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
        assertRemainingTTL(ttl1, 70, 100);
        assertTrue(filter.contains("1"));
        assertEquals(1, Math.round(filter.getEstimatedPopulation()));

        Thread.sleep(30);

        long ttl2 = filter.getRemainingTTL("1", MILLISECONDS);
        assertRemainingTTL(ttl2, 15, 70);

        Thread.sleep(150);

        Long ttl3 = filter.getRemainingTTL("1", MILLISECONDS);
        assertEquals(null, ttl3);
        assertFalse("Element (1) should not be contained in Bloom filter", filter.contains("1"));
    }

    @Test
    public void exceedCapacity() {
        FilterBuilder b = new FilterBuilder(100, 0.05).overwriteIfExists(true);
        createFilter(b);

        IntStream.range(0, 200).forEach(i -> {
            String elem = String.valueOf(i);
            filter.reportRead(elem, 1000, TimeUnit.SECONDS);
            filter.reportWrite(elem);
            assertTrue(filter.contains(elem));
            //System.out.println(filter.getEstimatedPopulation() + ":" + filter.getEstimatedFalsePositiveProbability());
        });
        //System.out.println(filter.getFalsePositiveProbability(200));

        //Materialized size roughly equal to estimated size
        assertTrue(Math.abs(filter.getEstimatedPopulation() - 200) < 10);
        //fpp exceeded
        assertTrue(filter.getEstimatedFalsePositiveProbability() > 0.05);
        //Less then 10% difference between estimated and precise fpp
        assertTrue(Math.abs(
            1 - filter.getEstimatedFalsePositiveProbability() / filter.getFalsePositiveProbability(200)) < 0.1);
    }

    @Test
    public void testClone() throws Exception {
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
    public void testReportMultipleWrites() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, MILLISECONDS);
        filter.reportRead("2", 50, MILLISECONDS);
        filter.reportWrites(Arrays.asList("1","2"));
        assertTrue(filter.contains("1"));
        assertTrue(filter.contains("2"));
    }

    @Test
    public void testClear() throws Exception {
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
    public void testGetTtls() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String key = String.valueOf(i);
            keys.add(key);
            filter.reportRead(key, 50, TimeUnit.SECONDS);
        }

        List<Long> ttls = filter.getRemainingTTLs(keys, TimeUnit.SECONDS);

        for (Long ttl : ttls) {
            assertTrue(ttl >= 49);
        }
    }

    @Test
    public void testGetTimeToLiveMap() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        filter.reportRead("Foo", 2, TimeUnit.SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 4, TimeUnit.SECONDS);
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        filter.reportRead("Baz", 3, TimeUnit.SECONDS);
        filter.reportWrite("Baz");
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));

        TimeMap<String> map = filter.getTimeToLiveMap();
        assertEquals(3, map.size());
        assertTrue(map.containsKey("Foo"));
        assertTimeBetween(1, 2, TimeUnit.SECONDS, map, "Foo");
        assertTrue(map.containsKey("Bar"));
        assertTimeBetween(3, 4, TimeUnit.SECONDS, map, "Bar");
        assertTrue(map.containsKey("Baz"));
        assertTimeBetween(2, 3, TimeUnit.SECONDS, map, "Baz");
    }

    @Test
    public void testSetTimeToLiveMap() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        assertFalse(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.isCached("Baz"));
        assertFalse(filter.contains("Baz"));

        TimeMap<String> map = new TimeMap<>();
        map.putRemaining("Foo", 2L, TimeUnit.SECONDS);
        map.putRemaining("Bar", 4L, TimeUnit.SECONDS);
        map.putRemaining("Baz", 3L, TimeUnit.SECONDS);

        filter.setTimeToLiveMap(map);

        assertTrue(filter.isCached("Foo"));
        assertTrue(1000L < filter.getRemainingTTL("Foo", TimeUnit.MILLISECONDS));
        assertFalse(filter.contains("Foo"));

        assertTrue(filter.isCached("Bar"));
        assertTrue(3000L < filter.getRemainingTTL("Bar", TimeUnit.MILLISECONDS));
        assertFalse(filter.contains("Bar"));

        assertTrue(filter.isCached("Baz"));
        assertTrue(2000L < filter.getRemainingTTL("Baz", TimeUnit.MILLISECONDS));
        assertFalse(filter.contains("Baz"));
    }

    @Test
    public void testGetExpirationMap() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        filter.reportRead("Foo", 2, TimeUnit.SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 4, TimeUnit.SECONDS);
        filter.reportWrite("Bar");
        assertTrue(filter.isCached("Bar"));
        assertTrue(filter.contains("Bar"));
        filter.reportRead("Baz", 3, TimeUnit.SECONDS);
        filter.reportWrite("Baz");
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));

        TimeMap<String> map = filter.getExpirationMap();
        assertEquals(2, map.size());
        assertFalse(map.containsKey("Foo"));
        assertTrue(map.containsKey("Bar"));
        assertTimeBetween(3, 4, TimeUnit.SECONDS, map, "Bar");
        assertTrue(map.containsKey("Baz"));
        assertTimeBetween(2, 3, TimeUnit.SECONDS, map, "Baz");
    }

    @Test
    public void testSetExpirationMap() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        createFilter(b);

        assertFalse(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        assertFalse(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertFalse(filter.isCached("Baz"));
        assertFalse(filter.contains("Baz"));

        TimeMap<String> map = new TimeMap<>();
        map.putRemaining("Foo", 2L, TimeUnit.SECONDS);
        map.putRemaining("Bar", 4L, TimeUnit.SECONDS);
        map.putRemaining("Baz", 3L, TimeUnit.SECONDS);

        filter.setExpirationMap(map);

        TimeMap<String> actualMap = filter.getExpirationMap();
        assertEquals(map, actualMap);
    }

    @Test
    public void testMigrateFromInMemoryExpiringBloomFilter() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);

        // Create the filter to migrate from
        ExpiringBloomFilterMemory<String> inMemory = new ExpiringBloomFilterMemory<>(b);
        inMemory.reportRead("Foo", 50, TimeUnit.SECONDS);
        assertTrue(inMemory.isCached("Foo"));
        assertFalse(inMemory.contains("Foo"));
        inMemory.reportRead("Bar", 40, TimeUnit.SECONDS);
        assertTrue(inMemory.isCached("Bar"));
        assertFalse(inMemory.contains("Bar"));
        inMemory.reportRead("Baz", 30, TimeUnit.SECONDS);
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
        assertRemainingTTL(filter.getRemainingTTL("Foo", TimeUnit.SECONDS), 30, 50);
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        assertRemainingTTL(filter.getRemainingTTL("Bar", TimeUnit.SECONDS), 20, 40);
        assertTrue(filter.isCached("Baz"));
        assertTrue(filter.contains("Baz"));
        assertRemainingTTL(filter.getRemainingTTL("Baz", TimeUnit.SECONDS), 10, 30);

        // Cleanup in-memory BF
        inMemory.clear();
    }

    @Test
    public void testMigrateToInMemoryExpiringBloomFilter() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.001);

        // Create the filter to migrate from
        createFilter(b);
        filter.reportRead("Foo", 3, TimeUnit.SECONDS);
        assertTrue(filter.isCached("Foo"));
        assertFalse(filter.contains("Foo"));
        filter.reportRead("Bar", 2, TimeUnit.SECONDS);
        assertTrue(filter.isCached("Bar"));
        assertFalse(filter.contains("Bar"));
        filter.reportRead("Baz", 1, TimeUnit.SECONDS);
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
        assertRemainingTTL(inMemory.getRemainingTTL("Foo", TimeUnit.SECONDS), 2, 2);
        assertTrue(inMemory.isCached("Bar"));
        assertFalse(inMemory.contains("Bar"));
        assertRemainingTTL(inMemory.getRemainingTTL("Bar", TimeUnit.SECONDS), 1, 2);
        assertTrue(inMemory.isCached("Baz"));
        assertTrue(inMemory.contains("Baz"));
        assertRemainingTTL(inMemory.getRemainingTTL("Baz", TimeUnit.SECONDS), 0, 1);

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

    private void assertRemainingTTL(long ttl, long min, long max) {
        assertTrue("Assert remaining TTL is lower than " + max + " ms, but was " + ttl + " ms", ttl <= max);
        assertTrue("Assert remaining TTL is higher than " + min + " ms, but was " + ttl + " ms", ttl >= min);
    }

    private void assertItemExists(Collection<ExpirationQueue.ExpiringItem<String>> actual, String expected, long expirationMin, long expirationMax, TimeUnit unit) {
        long min = MILLISECONDS.convert(expirationMin, unit);
        long max = MILLISECONDS.convert(expirationMax, unit);
        String u = unit.toString().toLowerCase();
        String message = "Expect expirations to contain " + expected + " with a TTL from " + expirationMin + " to " + expirationMax + " " + u;
        assertTrue(message, actual.stream().anyMatch(item -> {
            if (item.getItem().equals(expected)) {
                long expiration = item.getExpiration(MILLISECONDS);
                return expiration >= min && expiration <= max;
            }

            return false;
        }));
    }

    private <T> void assertTimeBetween(long min, long max, TimeUnit expectedUnit, TimeMap<T> actual, T actualItem) {
        Long remaining = actual.getRemaining(actualItem, MILLISECONDS);
        assertNotNull(remaining);
        long minNormalized = MILLISECONDS.convert(min, expectedUnit);
        long maxNormalized = MILLISECONDS.convert(max, expectedUnit);
        assertTrue("Expect " + remaining + "ms to be higher than or equal to " + minNormalized + "ms", remaining >= minNormalized);
        assertTrue("Expect " + remaining + "ms to be less than or equal to " + maxNormalized + "ms", remaining <= maxNormalized);
    }
}
