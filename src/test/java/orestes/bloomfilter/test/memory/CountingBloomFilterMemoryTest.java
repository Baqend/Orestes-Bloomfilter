package orestes.bloomfilter.test.memory;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Constructor;
import java.util.*;

import static junit.framework.TestCase.*;
import static orestes.bloomfilter.test.helper.Helper.*;

/**
 * Created on 26.09.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
@RunWith(Parameterized.class)
public class CountingBloomFilterMemoryTest {
    private final int countingBits;
    private final Class<? extends CountingBloomFilterMemory<String>> cbfClass;
    private CountingBloomFilterMemory<String> cbf;

    @Parameterized.Parameters(name = "Counting Bloom filter test with {0} bits and {1}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {
            { 8, CountingBloomFilter8.class},
            {16, CountingBloomFilter16.class},
            {32, CountingBloomFilter32.class},
            {64, CountingBloomFilter64.class},
            { 3, CountingBloomFilterMemory.class},
            { 7, CountingBloomFilterMemory.class},
            { 8, CountingBloomFilterMemory.class},
            {16, CountingBloomFilterMemory.class},
            {32, CountingBloomFilterMemory.class},
            {64, CountingBloomFilterMemory.class},
        };

        return Arrays.asList(data);
    }

    public CountingBloomFilterMemoryTest(int countingBits, Class<? extends CountingBloomFilterMemory<String>> cbfClass) {
        this.countingBits = countingBits;
        this.cbfClass = cbfClass;
    }

    @Before
    public void setUp() throws Exception {
        Constructor<? extends CountingBloomFilterMemory<String>> constructor = cbfClass.getConstructor(FilterBuilder.class);
        cbf = constructor.newInstance(configure(1000, 0.02, HashMethod.MD5).countingBits(countingBits));
    }

    @Test
    public void testCreate() {
        // Assert the Bloom filter is empty in the beginning
        assertEquals(8143, cbf.config().size());
        assertEquals(6, cbf.config().hashes());
        assertEquals(countingBits, cbf.config().countingBits());
        assertEquals(0, cbf.getBitSet().cardinality());
        assertEquals(0, cbf.getBitSet().length());
        assertEquals(Collections.emptyMap(), cbf.getCountMap());
    }

    @Test
    public void testSet() {
        // Add the entry
        assertFalse(cbf.contains("foo"));
        assertTrue(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));

        // Test the bits set
        BitSet bitSet = cbf.getBitSet();
        assertBitsSet(bitSet, 4484, 4918, 5583, 6134, 6341, 7596);

        // Test the new count map
        Map<Integer, Long> countMap = cbf.getCountMap();
        assertAllEqual(1L, countMap, 4484, 4918, 5583, 6134, 6341, 7596);
    }

    @Test
    public void testAdd() {
        // Add the first entry
        assertFalse(cbf.contains("foo"));
        assertTrue(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));

        // Add the second entry
        assertFalse(cbf.contains("bar"));
        assertTrue(cbf.add("bar"));
        assertTrue(cbf.contains("bar"));
        assertEquals(1L, cbf.getEstimatedCount("bar"));

        // Test the bits set
        BitSet bitSet = cbf.getBitSet();
        assertBitsSet(bitSet, 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);

        // Test the new count map
        Map<Integer, Long> countMap = cbf.getCountMap();
        assertAllEqual(1L, countMap, 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);

        // Add the second foo entry
        assertTrue(cbf.contains("foo"));
        assertFalse(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(2L, cbf.getEstimatedCount("foo"));


        // Test the bits set
        assertBitsSet(bitSet, 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);

        // Test the new count map
        Map<Integer, Long> countMap2 = cbf.getCountMap();
        assertEquals(12, countMap2.size());
        assertEquals(1L, (long) countMap2.get(1770));
        assertEquals(1L, (long) countMap2.get(2285));
        assertEquals(1L, (long) countMap2.get(2861));
        assertEquals(2L, (long) countMap2.get(4484));
        assertEquals(1L, (long) countMap2.get(4742));
        assertEquals(2L, (long) countMap2.get(4918));
        assertEquals(1L, (long) countMap2.get(5431));
        assertEquals(2L, (long) countMap2.get(5583));
        assertEquals(2L, (long) countMap2.get(6134));
        assertEquals(2L, (long) countMap2.get(6341));
        assertEquals(2L, (long) countMap2.get(7596));
        assertEquals(1L, (long) countMap2.get(7745));
    }

    @Test
    public void testAddTwice() {
        // Add the first entry
        assertFalse(cbf.contains("foo"));
        assertTrue(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));
        assertBitsSet(cbf.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(1L, cbf.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);

        // Add the second entry
        assertFalse(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(2L, cbf.getEstimatedCount("foo"));
        assertBitsSet(cbf.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(2L, cbf.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);
    }

    @Test
    public void testUnset() {
        // Add the entry
        assertFalse(cbf.contains("foo"));
        assertTrue(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));

        // Remove the entry
        assertTrue(cbf.remove("foo"));
        assertFalse(cbf.contains("foo"));
        assertEquals(0L, cbf.getEstimatedCount("foo"));

        // Test the bits set
        BitSet bitSet = cbf.getBitSet();
        assertEquals(0, bitSet.cardinality());

        // Test the new count map
        Map<Integer, Long> countMap = cbf.getCountMap();
        assertEquals(0, countMap.size());
    }

    @Test
    public void testRemove() {
        // Add the entry once
        assertFalse(cbf.contains("foo"));
        assertTrue(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));

        // Add the entry twice
        assertFalse(cbf.add("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(2L, cbf.getEstimatedCount("foo"));

        // Remove the entry once
        assertFalse(cbf.remove("foo"));
        assertTrue(cbf.contains("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));

        // Remove the entry twice
        assertTrue(cbf.remove("foo"));
        assertFalse(cbf.contains("foo"));
        assertEquals(0L, cbf.getEstimatedCount("foo"));

        // Test the bits set
        BitSet bitSet = cbf.getBitSet();
        assertEquals(0, bitSet.cardinality());

        // Test the new count map
        Map<Integer, Long> countMap = cbf.getCountMap();
        assertEquals(0, countMap.size());
    }

    @Test
    public void testOverflow() {
        assertEquals(0L, cbf.getEstimatedCount("foo"));
        assertTrue(cbf.add("foo"));
        assertEquals(1L, cbf.getEstimatedCount("foo"));

        if (countingBits <= 16) {
            for (long i = 2L; i < (1L << countingBits); i++) {
                assertFalse(cbf.add("foo"));
                assertEquals(i, cbf.getEstimatedCount("foo"));
            }

            // Set an overflow handler
            boolean[] called = {false};
            cbf.setOverflowHandler(() -> called[0] = true);
            assertFalse(called[0]);

            // Cause an overflow
            assertFalse(cbf.add("foo"));
            assertEquals((1 << countingBits) - 1, cbf.getEstimatedCount("foo"));

            // Ensure overflow handler was called
            assertTrue(called[0]);
        }
    }
}
