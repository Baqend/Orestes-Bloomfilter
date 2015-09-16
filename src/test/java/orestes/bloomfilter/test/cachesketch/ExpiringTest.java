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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(Parameterized.class)
public class ExpiringTest {
    private final boolean inMemory;
    private ExpiringBloomFilter<String> filter;

    @Parameterized.Parameters(name = "Expiring Bloom Filter test {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {
            {"in-memory", true},
            {"with redis", false}
        };

        return Arrays.asList(data);
    }

    public ExpiringTest(String name, boolean inMemory) {
        this.inMemory = inMemory;
    }

    public <T> void createFilter(FilterBuilder b) {
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
        FilterBuilder b = new FilterBuilder(1000, 0.05);
        createFilter(b);
        int rounds = 100;
        ExecutorService threads = Executors.newFixedThreadPool(rounds);
        CountDownLatch latch = new CountDownLatch(rounds);

        for (int i = 0; i < rounds; i++) {
            final String item = String.valueOf(i);
            CompletableFuture.runAsync(() -> {
                filter.reportRead(item, 100, TimeUnit.MILLISECONDS);
                assertTrue(filter.isCached(item));
                assertTrue(filter.getRemainingTTL(item, TimeUnit.MILLISECONDS) >= 0);
                assertFalse(filter.contains(item));
                boolean invalidation = filter.reportWrite(item);
                assertTrue(invalidation);
                assertTrue(filter.contains(item));
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                assertFalse(filter.contains(item));
                assertEquals(null, filter.getRemainingTTL(item, TimeUnit.MILLISECONDS));
                latch.countDown();
            }, threads);
        }

        latch.await(3, TimeUnit.SECONDS);
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
        assertTrue(Math.abs(1 - filter.getEstimatedFalsePositiveProbability() / filter.getFalsePositiveProbability(200)) < 0.1);
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
}
