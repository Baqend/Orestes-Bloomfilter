package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

/**
 * Created on 2018-09-25.
 *
 * @author Erik Witt
 */
public class ExpiringBloomFilterMemoryTest {
    private ExpiringBloomFilterMemory bloomFilter;
    private RedisPool redis;
    private RedisKeys keys;

    @Before
    public void setUp() throws Exception {
        FilterBuilder builder = new FilterBuilder(100, 0.01);
        builder
            .overwriteIfExists(true)
            .gracePeriod(1, TimeUnit.SECONDS);
        bloomFilter = new ExpiringBloomFilterMemory(builder);
    }

    @After
    public void teardown() throws Exception {
        bloomFilter.clear();
    }

    @Test
    public void testEmptyBefore() {
        assertClear();
    }

    @Test
    public void testEmptyAfterClear() {
        assertClear();

        // Test state after read
        bloomFilter.reportRead("hello", 1, TimeUnit.SECONDS);
        assertEmpty();

        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));

        // Test state after clear
        bloomFilter.clear();
        assertClear();
        assertFalse(bloomFilter.isKnown("hello"));
    }

    @Test
    public void testReportReadExpires() throws Exception {
        assertClear();

        // Test state after read
        bloomFilter.reportRead("hello", 1, TimeUnit.SECONDS);
        assertEmpty();

        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after timeout
        sleep(1_100);
        assertEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after grace period timeout
        sleep(1_100);
        bloomFilter.cleanupTTLs();
        assertEmpty();
        assertHasNoTtl("hello");
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));
    }

    @Test
    public void testReportWriteExpires() throws Exception {
        assertClear();
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after read
        bloomFilter.reportRead("hello", 1, TimeUnit.SECONDS);
        assertEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after write
        bloomFilter.reportWrite("hello");
        assertNonEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertTrue(bloomFilter.contains("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertNonEmpty();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertTrue(bloomFilter.contains("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after timeout
        sleep(1_100);
        assertEquals(0, bloomFilter.getBitSet().cardinality());
        assertCountMapEmpty();

        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertFalse(bloomFilter.contains("hello"));
        assertTrue(bloomFilter.getExpirationMap().isEmpty());
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertFalse(bloomFilter.contains("hello"));
        assertTrue(bloomFilter.getExpirationMap().isEmpty());
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after grace period timeout
        sleep(1_100);
        bloomFilter.cleanupTTLs();
        assertHasNoTtl("hello");
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertFalse(bloomFilter.contains("hello"));
        assertTrue(bloomFilter.getExpirationMap().isEmpty());
    }

    private void assertHasTtl(String key) {
        assertTrue(bloomFilter.getTimeToLiveMap().containsKey(key));
    }

    private void assertHasNoTtl(String key) {
        assertFalse(bloomFilter.getTimeToLiveMap().containsKey(key));;
    }

    /**
     * Asserts that the whole Bloom filter is cleared
     */
    private void assertClear() {
        assertTrue(bloomFilter.getTimeToLiveMap().isEmpty());
        assertEquals(0.0, bloomFilter.getEstimatedPopulation(), 0.1);
        assertEmpty();
    }

    /**
     * Asserts that the Bloom filter is empty
     */
    private void assertEmpty() {
        assertTrue(bloomFilter.getExpirationMap().isEmpty());
        assertTrue(bloomFilter.getBitSet().isEmpty());
        assertTrue(bloomFilter.getCountMap().isEmpty());
    }

    /**
     * Asserts that the Bloom filter is not empty
     */
    private void assertNonEmpty() {
        assertFalse(bloomFilter.getExpirationMap().isEmpty());
        assertFalse(bloomFilter.getBitSet().isEmpty());
        assertFalse(bloomFilter.getCountMap().isEmpty());
    }

    /**
     * Asserts that the Bloom filter count map is empty.
     */
    private void assertCountMapEmpty() {
        long countMapSum = bloomFilter.getCountMap().values().stream().mapToLong(value -> (Long) value).sum();
        assertEquals(0, countMapSum);
    }
}
