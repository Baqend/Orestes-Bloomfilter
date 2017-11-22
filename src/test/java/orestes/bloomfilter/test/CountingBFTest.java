package orestes.bloomfilter.test;

import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.redis.RedisUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static orestes.bloomfilter.test.helper.Helper.createCountingFilter;
import static orestes.bloomfilter.test.helper.Helper.createCountingRedisFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CountingBFTest {
    private final boolean redis;
    private static final String name = "CountingTests";

    @Parameterized.Parameters(name = "Counting Bloom Filter test with {0}")
    public static Collection<Object[]> data() throws Exception {
        Object[][] data = {{"memory", false}, {"redis", true}};
        return Arrays.asList(data);
    }

    public CountingBFTest(String name, boolean redis) {
        this.redis = redis;
    }

    private CountingBloomFilter<String> createFilter(String name, int n, double p, HashMethod hm) {
        if (!redis) {
            return createCountingFilter(n, p, hm);
        } else {
            return createCountingRedisFilter(name, n, p, hm, true);
        }
    }

    @Test
    public void testCounterSizes() {
        int m = 100000;
        int k = 10;
        Stream.of(4, 8, 16, 32, 55, 64).forEach(bits -> {
            CountingBloomFilter<String> filter = new FilterBuilder(m, k).name(name).redisBacked(redis).countingBits(bits).buildCountingBloomFilter();
            filter.clear();

            long first = filter.addAndEstimateCount("first");
            long second = filter.addAndEstimateCount("first");
            long other = filter.addAndEstimateCount("other");
            filter.removeAll(Arrays.asList("first","first","other"));

            long big = 0;
            long inserts = (long) Math.min(Math.pow(2, 8), Math.pow(2, bits)-1);
            for (int i = 1; i <= inserts; i++) {
                big = filter.addAndEstimateCount("muuh");
                assertEquals(i, big);
            }

            assertEquals(1, first);
            assertEquals(1, other);
            assertEquals(2, second);
        });

    }

    @Test
    public void testCardinality() {
        int n = 1000;
        double p = 0.01;
        int elements = 100;
        int range = 20;
        CountingBloomFilter<String> b = createFilter(name + "normal", n, p, HashMethod.MD5);
        Random r = new Random();
        List<String> adds = r.longs().limit(elements).mapToObj(i -> String.valueOf(i % range)).collect(Collectors.toList());

        Map<String, Long> counters = new HashMap<>();
        //Check that counting add is correct
        for (String add : adds) {
            counters.compute(add, (k, v) -> (v == null) ? 1 : (v + 1));
            Long count = b.addAndEstimateCount(add);
            assertEquals(counters.get(add), count);
        }

        //check that estimate count is correct
        for (String added : adds) {
            long expected = adds.stream().filter(e -> e.equals(added)).count();
            long actual = b.getEstimatedCount(added);
            assertEquals(expected, actual);
        }

        //Check that counting remove is correct
        for (String add : adds) {
            counters.computeIfPresent(add, (k, v) -> v - 1);
            Long count = b.removeAndEstimateCount(add);
            assertEquals(counters.get(add), count);
        }

        assertTrue(b.isEmpty());

        b.remove();
    }

    @Test
    public void countingTest() {
        int n = 5;
        double p = 0.01;
        CountingBloomFilter<String> b = createFilter(name + "normal", n, p, HashMethod.MD5);
        System.out.println("Size of bloom filter: " + b.getSize() + ", hash functions: " + b.getHashes());
        b.add("Käsebrot");
        b.add("ist");
        b.add("ein");
        b.add("gutes");
        b.add("Brot");
        assertTrue(b.contains("Käsebrot"));
        assertTrue(b.contains("ist"));
        assertTrue(b.contains("ein"));
        assertTrue(b.contains("gutes"));
        assertTrue(b.contains("Brot"));
        assertTrue(!b.contains("Kartoffelsalate"));
        assertTrue(!b.contains("Dachlatte"));
        assertTrue(!b.contains("Die Sechszehnte"));
        b.remove("Käsebrot");
        b.remove("ist");
        b.remove("ein");
        b.remove("gutes");
        b.remove("Brot");
        assertTrue(!b.contains("Käsebrot"));
        assertTrue(!b.contains("ist"));
        assertTrue(!b.contains("ein"));
        assertTrue(!b.contains("gutes"));
        assertTrue(!b.contains("Brot"));
        b.remove();
    }

    @Test
    public void countingBasics() {
        int n = 2;
        double p = 0.01;
        CountingBloomFilter<String> b = createFilter(name + "normal", n, p, HashMethod.MD5);
        System.out.println("Size of bloom filter: " + b.getSize() + ", hash functions: " + b.getHashes());
        b.add("Käsebrot");
        assertTrue(b.contains("Käsebrot"));
        b.remove("Käsebrot");
        assertTrue(!b.contains("Käsebrot"));
        b.add("Schnitte");
        b.add("Schnitte");
        assertTrue(b.contains("Schnitte"));
        b.remove("Schnitte");
        assertTrue(b.contains("Schnitte"));
        b.remove("Schnitte");
        assertTrue(!b.contains("Schnitte"));
        CountingBloomFilter<String> bc = b.clone();
        assertTrue(b.equals(bc));
        b.remove();
    }
}
