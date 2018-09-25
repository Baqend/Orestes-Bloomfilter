package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.TimeMap;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

/**
 * Created on 2018-09-25.
 *
 * @author Konstantin Simon Maria Möllers
 */
class ExpiringTestHelpers {
    static <T> void assertTimeBetween(long min, long max, TimeUnit expectedUnit, TimeMap<T> actual, T actualItem) {
        Long remaining = actual.getRemaining(actualItem, MILLISECONDS);
        assertNotNull(remaining);

        // ttls are handle as doubles by redis, therefore we must expect some rounding issues
        long minNormalized = MILLISECONDS.convert(min, expectedUnit) - 1;
        long maxNormalized = MILLISECONDS.convert(max, expectedUnit) + 1;

        assertBetween("Expect " + remaining + "ms to be between " + minNormalized + "ms and " + maxNormalized + "ms", minNormalized, maxNormalized, remaining);
    }

    static void assertRemainingTTL(long expectedLowerBound, long expectedUpperBound, long actualTTL) {
        String message = "Expect " + actualTTL + "ms to be between " + expectedLowerBound + "ms and " + expectedUpperBound + "ms";
        assertBetween(message, expectedLowerBound, expectedUpperBound, actualTTL);
    }

    static void assertLessThan(double expectedUpperBound, double actual) {
        assertTrue(actual < expectedUpperBound);
    }

    static void assertGreaterThan(double expectedLowerBound, double actual) {
        assertTrue(actual > expectedLowerBound);
    }

    static void assertBetween(long expectedLowerBound, long expectedUpperBound, long actual) {
        assertTrue(actual >= expectedLowerBound);
        assertTrue(actual <= expectedUpperBound);
    }

    static void assertBetween(String message, long expectedLowerBound, long expectedUpperBound, long actual) {
        assertTrue(message, actual >= expectedLowerBound);
        assertTrue(message, actual <= expectedUpperBound);
    }

    static <T> void assertEmpty(BloomFilter<T> actualBloomFilter) {
        assertTrue(actualBloomFilter.isEmpty());
        assertEquals(0, actualBloomFilter.getBitSet().cardinality());
        assertEquals(0, actualBloomFilter.getBitSet().length());
    }
}
