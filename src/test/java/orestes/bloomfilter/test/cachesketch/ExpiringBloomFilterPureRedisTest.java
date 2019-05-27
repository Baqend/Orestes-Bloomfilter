package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;
import static java.lang.Thread.sleep;

/**
 * Created on 2018-09-21.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class ExpiringBloomFilterPureRedisTest {
    private ExpiringBloomFilterPureRedis bloomFilter;
    private RedisPool redis;
    private RedisKeys keys;

    @Before
    public void setUp() throws Exception {
        FilterBuilder builder = new FilterBuilder(100, 0.01);
        builder
            .overwriteIfExists(true)
            .gracePeriod(1, SECONDS);
        bloomFilter = new ExpiringBloomFilterPureRedis(builder);
        redis = bloomFilter.getRedisPool();
        keys = bloomFilter.getRedisKeys();
    }

    @After
    public void teardown() throws Exception {
        bloomFilter.clear();
    }

    @Test
    public void testEmptyBefore() {
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.TTL_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
    }

    @Test
    public void testEmptyAfterClear() {
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.TTL_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);

        // Test state after read
        bloomFilter.reportRead("hello", 1, SECONDS);
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertExists(keys.TTL_KEY);
        assertNotNull(redis.safelyReturn(r -> r.zscore(keys.TTL_KEY, "hello")));
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));

        // Test state after clear
        bloomFilter.clear();
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNull(redis.safelyReturn(r -> r.zscore(keys.TTL_KEY, "hello")));
        assertNotExists(keys.TTL_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
    }

    @Test
    public void testReportReadExpires() throws Exception {
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.TTL_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after read
        bloomFilter.reportRead("hello", 1, SECONDS);
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after timeout
        sleep(1_100);
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after grace period timeout
        sleep(1_100);
        bloomFilter.cleanupTTLs();
        assertHasNoTtl("hello");
        assertNotExists(keys.TTL_KEY);
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));
    }

    @Test
    public void testReportWriteExpires() throws Exception {
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.TTL_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after read
        bloomFilter.reportRead("hello", 1, SECONDS);
        assertNotExists(keys.BITS_KEY);
        assertNotExists(keys.COUNTS_KEY);
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after write
        bloomFilter.reportWrite("hello");
        assertExists(keys.BITS_KEY);
        assertExists(keys.COUNTS_KEY);
        assertExists(keys.EXPIRATION_QUEUE_KEY);
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertTrue(bloomFilter.isCached("hello"));
        assertNotNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after timeout
        sleep(1_100);
        assertEquals(0L, (long) redis.safelyReturn(r -> r.bitcount(keys.BITS_KEY)));
        assertEquals(0L, (long) redis.safelyReturn(r -> r.hlen(keys.COUNTS_KEY)));
        assertHasTtl("hello");
        assertNotExists(keys.EXPIRATION_QUEUE_KEY);
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after cleaning TTLs
        bloomFilter.cleanupTTLs();
        assertHasTtl("hello");
        assertTrue(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));

        // Test state after grace period timeout
        sleep(1_100);
        bloomFilter.cleanupTTLs();
        assertHasNoTtl("hello");
        assertNotExists(keys.TTL_KEY);
        assertFalse(bloomFilter.isKnown("hello"));
        assertFalse(bloomFilter.isCached("hello"));
        assertNull(bloomFilter.getRemainingTTL("hello", TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUpdateConfig() throws Exception {
        FilterBuilder builder = new FilterBuilder();
        builder
                .name(bloomFilter.config().name())
                .gracePeriod(1337)
                .falsePositiveProbability(1337)
                .size(1337)
                .hashes(1337)
                .expectedElements(1337)
                .countingBits(1337)
                .hashFunction(HashProvider.HashMethod.CRC32);

        ExpiringBloomFilterPureRedis bf = new ExpiringBloomFilterPureRedis(builder);

        assertNotEquals(bf.config().gracePeriod(), bloomFilter.config().gracePeriod());
        assertEquals(1337, bf.config().gracePeriod());

        assertEquals(bf.config().falsePositiveProbability(), bloomFilter.config().falsePositiveProbability(), 0);
        assertEquals(bf.config().size(), bloomFilter.config().size());
        assertEquals(bf.config().hashes(), bloomFilter.config().hashes());
        assertEquals(bf.config().expectedElements(), bloomFilter.config().expectedElements());
        assertEquals(bf.config().countingBits(), bloomFilter.config().countingBits());
        assertEquals(bf.config().hashMethod().name(), bloomFilter.config().hashMethod().name());
    }

    private void assertExists(String key) {
        assertTrue(redis.safelyReturn(r -> r.exists(key)));
    }

    private void assertNotExists(String key) {
        assertFalse(redis.safelyReturn(r -> r.exists(key)));
    }

    private void assertHasTtl(String key) {
        assertExists(keys.TTL_KEY);
        assertNotNull(redis.safelyReturn(r -> r.zscore(keys.TTL_KEY, key)));
    }

    private void assertHasNoTtl(String key) {
        assertNull(redis.safelyReturn(r -> r.zscore(keys.TTL_KEY, key)));
    }
}
