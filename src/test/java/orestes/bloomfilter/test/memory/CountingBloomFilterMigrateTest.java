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
public class CountingBloomFilterMigrateTest {
    private final int countingBits;
    private final Class<? extends CountingBloomFilterMemory<String>> cbfClass;
    private CountingBloomFilterMemory<String> cbf1;
    private CountingBloomFilterMemory<String> cbf2;

    @Parameterized.Parameters(name = "Counting Bloom filter migrate test with {0} bits and {1}")
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

    public CountingBloomFilterMigrateTest(int countingBits, Class<? extends CountingBloomFilterMemory<String>> cbfClass) {
        this.countingBits = countingBits;
        this.cbfClass = cbfClass;
    }

    @Before
    public void setUp() throws Exception {
        Constructor<? extends CountingBloomFilterMemory<String>> constructor = cbfClass.getConstructor(FilterBuilder.class);
        cbf1 = constructor.newInstance(configure(1000, 0.02, HashMethod.MD5).countingBits(countingBits));
        cbf2 = constructor.newInstance(configure(1000, 0.02, HashMethod.MD5).countingBits(countingBits));
    }

    @Test
    public void testCreate() {
        // Assert the Bloom filter is empty in the beginning
        assertEquals(8143, cbf1.config().size());
        assertEquals(8143, cbf2.config().size());
        assertEquals(6, cbf1.config().hashes());
        assertEquals(6, cbf2.config().hashes());
        assertEquals(countingBits, cbf1.config().countingBits());
        assertEquals(countingBits, cbf2.config().countingBits());
        assertEquals(0, cbf1.getBitSet().cardinality());
        assertEquals(0, cbf2.getBitSet().cardinality());
        assertEquals(0, cbf1.getBitSet().length());
        assertEquals(0, cbf2.getBitSet().length());
        assertEquals(Collections.emptyMap(), cbf1.getCountMap());
        assertEquals(Collections.emptyMap(), cbf2.getCountMap());
        assertTrue(cbf1.compatible(cbf2));
    }

    @Test
    public void testMigrateEmpty() {
        assertSame(cbf2, cbf1.migrateTo(cbf2));

        assertEquals(0, cbf1.getBitSet().cardinality());
        assertEquals(0, cbf2.getBitSet().cardinality());
        assertEquals(0, cbf1.getBitSet().length());
        assertEquals(0, cbf2.getBitSet().length());
        assertEquals(Collections.emptyMap(), cbf1.getCountMap());
        assertEquals(Collections.emptyMap(), cbf2.getCountMap());
    }

    @Test
    public void testMigrateOneEntry() {
        // Add the entry
        assertFalse(cbf1.contains("foo"));
        assertTrue(cbf1.add("foo"));
        assertTrue(cbf1.contains("foo"));
        assertEquals(1L, cbf1.getEstimatedCount("foo"));
        assertBitsSet(cbf1.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(1L, cbf1.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);

        // Migrate to other Bloom filter
        assertSame(cbf2, cbf1.migrateTo(cbf2));

        // Check other Bloom filter
        assertTrue(cbf2.contains("foo"));
        assertEquals(1L, cbf2.getEstimatedCount("foo"));
        assertBitsSet(cbf2.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(1L, cbf2.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);
    }

    @Test
    public void testMigrateEntryTwice() {
        // Add the entry twice
        assertFalse(cbf1.contains("foo"));
        assertTrue(cbf1.add("foo"));
        assertFalse(cbf1.add("foo"));
        assertTrue(cbf1.contains("foo"));
        assertEquals(2L, cbf1.getEstimatedCount("foo"));
        assertBitsSet(cbf1.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(2L, cbf1.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);

        // Migrate to other Bloom filter
        assertSame(cbf2, cbf1.migrateTo(cbf2));

        // Check other Bloom filter
        assertTrue(cbf2.contains("foo"));
        assertEquals(2L, cbf2.getEstimatedCount("foo"));
        assertBitsSet(cbf2.getBitSet(), 4484, 4918, 5583, 6134, 6341, 7596);
        assertAllEqual(2L, cbf2.getCountMap(), 4484, 4918, 5583, 6134, 6341, 7596);
    }

    @Test
    public void testMigrateTwoEntries() {
        // Add the entries
        assertFalse(cbf1.contains("foo"));
        assertFalse(cbf1.contains("bar"));
        assertTrue(cbf1.add("foo"));
        assertTrue(cbf1.add("bar"));
        assertTrue(cbf1.contains("foo"));
        assertTrue(cbf1.contains("bar"));
        assertEquals(1L, cbf1.getEstimatedCount("foo"));
        assertEquals(1L, cbf1.getEstimatedCount("bar"));
        assertBitsSet(cbf1.getBitSet(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
        assertAllEqual(1L, cbf1.getCountMap(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);

        // Migrate to other Bloom filter
        assertSame(cbf2, cbf1.migrateTo(cbf2));

        // Check other Bloom filter
        assertTrue(cbf2.contains("foo"));
        assertTrue(cbf2.contains("bar"));
        assertEquals(1L, cbf2.getEstimatedCount("foo"));
        assertEquals(1L, cbf2.getEstimatedCount("bar"));
        assertBitsSet(cbf2.getBitSet(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
        assertAllEqual(1L, cbf2.getCountMap(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
    }

    @Test
    public void testMigrateTwoEntriesTwice() {
        // Add the entries
        assertFalse(cbf1.contains("foo"));
        assertFalse(cbf1.contains("bar"));
        assertTrue(cbf1.add("foo"));
        assertTrue(cbf1.add("bar"));
        assertFalse(cbf1.add("foo"));
        assertFalse(cbf1.add("bar"));
        assertTrue(cbf1.contains("foo"));
        assertTrue(cbf1.contains("bar"));
        assertEquals(2L, cbf1.getEstimatedCount("foo"));
        assertEquals(2L, cbf1.getEstimatedCount("bar"));
        assertBitsSet(cbf1.getBitSet(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
        assertAllEqual(2L, cbf1.getCountMap(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);

        // Migrate to other Bloom filter
        assertSame(cbf2, cbf1.migrateTo(cbf2));

        // Check other Bloom filter
        assertTrue(cbf2.contains("foo"));
        assertTrue(cbf2.contains("bar"));
        assertEquals(2L, cbf2.getEstimatedCount("foo"));
        assertEquals(2L, cbf2.getEstimatedCount("bar"));
        assertBitsSet(cbf2.getBitSet(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
        assertAllEqual(2L, cbf2.getCountMap(), 1770, 2285, 2861, 4484, 4742, 4918, 5431, 5583, 6134, 6341, 7596, 7745);
    }
}
