package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ExpiringTest {
    private final boolean inMemory;
    private ExpiringBloomFilter<String> filter;

    @Parameterized.Parameters(name = "Expiring Bloom Filter test {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {{"in-memory", true}, {"with redis", false}
        };

        return Arrays.asList(data);
    }

    public ExpiringTest(String name, boolean inMemory) {
        this.inMemory = inMemory;
    }

    public <T> void createFilter(FilterBuilder b) {
        b.overwriteIfExists(true);
        filter = inMemory ? new ExpiringBloomFilterMemory<>(b) : new ExpiringBloomFilterRedis<>(b);
        filter.clear();
    }

    @Before
    public void clear() {
        if (filter != null) {
            filter.clear();
        }
    }

    @Test
    public void addAndLetExpire() throws Exception {
        int rounds = 100;
        FilterBuilder b = new FilterBuilder(100000, 0.001);
        b.redisConnections(rounds);
        createFilter(b);
        ExecutorService threads = Executors.newFixedThreadPool(10);
        List<CompletableFuture> futures = new LinkedList<>();
        Random r = new Random(rounds);

        //Assert no collision
        for (int i = 0; i < rounds; i++) {
            final String item = String.valueOf(i);
            assertFalse(filter.contains(item));
            filter.add(item);
        }
        filter.clear();

        for (int i = 0; i < rounds; i++) {
            final int rand = r.nextInt(rounds);
            final String item = String.valueOf(i);
            futures.add(CompletableFuture.runAsync(() -> {
                int delay = (rounds - rand) * 10 + 1000;
                filter.reportRead(item, delay, TimeUnit.MILLISECONDS);
                assertTrue(filter.isCached(item));
                assertTrue(filter.getRemainingTTL(item, TimeUnit.MILLISECONDS) >= 0);
                assertFalse(filter.contains(item));
                boolean invalidation = filter.reportWrite(item);
                assertTrue(invalidation);
                assertTrue(filter.contains(item));
                try {
                    Thread.sleep(delay + 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Long remaining = filter.getRemainingTTL(item, TimeUnit.MILLISECONDS);
                if (filter.contains(item)) {
                    fail("Element still in BF. remaining TTL: " + remaining);
                }
                assertEquals(null, filter.getRemainingTTL(item, TimeUnit.MILLISECONDS));
            }, threads));
        }

        futures.forEach(CompletableFuture::join);
    }

    @Test
    public void testAddMultipleTimes() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        boolean invalidation = filter.reportWrite("1");
        filter.reportRead("1", 100, TimeUnit.MILLISECONDS);
        filter.reportRead("1", 800, TimeUnit.MILLISECONDS);
        filter.reportRead("1", 1500, TimeUnit.MILLISECONDS);
        filter.reportRead("1", 20, TimeUnit.MILLISECONDS);

        Long remainingTTL = filter.getRemainingTTL("1", TimeUnit.MILLISECONDS);

        assertFalse(invalidation);
        assertFalse(filter.contains("1"));
        assertTrue(remainingTTL <= 1500);
        assertTrue(remainingTTL > 1400);
    }


    @Test
    public void testExpiration() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, TimeUnit.MILLISECONDS);
        filter.reportRead("1", 100, TimeUnit.MILLISECONDS);
        Long ttl = filter.reportWrite("1", TimeUnit.MILLISECONDS);
        assertTrue(ttl <= 100);
        assertTrue(ttl >= 80);
        assertTrue(filter.contains("1"));
        assertEquals(1, Math.round(filter.getEstimatedPopulation()));
        Thread.sleep(30);
        assertTrue(filter.getRemainingTTL("1", TimeUnit.MILLISECONDS) <= 70);
        assertTrue(filter.getRemainingTTL("1", TimeUnit.MILLISECONDS) >= 50);
        Thread.sleep(100);
        assertEquals(null, filter.getRemainingTTL("1", TimeUnit.MILLISECONDS));
        assertFalse(filter.contains("1"));
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
        filter.reportRead("1", 50, TimeUnit.MILLISECONDS);
        filter.reportRead("2", 50, TimeUnit.MILLISECONDS);
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
        filter.reportRead("1", 50, TimeUnit.MILLISECONDS);
        filter.reportRead("2", 50, TimeUnit.MILLISECONDS);
        filter.reportWrites(Arrays.asList("1","2"));
        assertTrue(filter.contains("1"));
        assertTrue(filter.contains("2"));
    }

    @Test
    public void testClear() throws Exception {
        FilterBuilder b = new FilterBuilder(100000, 0.05);
        createFilter(b);
        filter.reportRead("1", 50, TimeUnit.MILLISECONDS);
        filter.reportRead("2", 50, TimeUnit.MILLISECONDS);
        filter.reportWrite("1");
        filter.clear();
        assertFalse(filter.contains("1"));
        assertFalse(filter.contains("2"));
        assertNull(filter.getRemainingTTL("1", TimeUnit.MILLISECONDS));
        assertNull(filter.getRemainingTTL("2", TimeUnit.MILLISECONDS));
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
}
