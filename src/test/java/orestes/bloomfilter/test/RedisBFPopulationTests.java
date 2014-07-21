package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.redis.BloomFilterRedis;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class RedisBFPopulationTests {

    private double defaultElements = 1_000;
    private double defaultError = 0.01;

    @Test
    public void emptyBF() {
        int defaultM = 5;
        int defaultK = 1000000;

        String filterName = "emptyTest";

        cleanupRedis(filterName);

        // Create a filter without the population counting
        //
        BloomFilterRedis<String> popFilter = BloomFilterRedis.createFilter(getConnection(), filterName, defaultM, defaultK, BloomFilter.HashMethod.Murmur);

        assertTrue(popFilter.getPopulation() == -1);

        cleanupRedis(filterName);

        // create a filter with population counting, but empty
        //
        popFilter = BloomFilterRedis.createPopulationFilter(getConnection(), filterName, defaultM, defaultK, BloomFilter.HashMethod.Murmur);

        assertTrue(popFilter.getPopulation() == 0);

        cleanupRedis(filterName);
    }

    @Test
    public void addTest() {
        String filterName = "addTest";

        cleanupRedis(filterName);
        BloomFilterRedis<String> popFilter = BloomFilterRedis.createFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        boolean added = popFilter.add("SecondTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == -1);

        cleanupRedis(filterName);
        popFilter = BloomFilterRedis.createPopulationFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        // popFilter = createFilter(filterName, hostname, port, defaultElements, defaultError, defaultFunction, true);
        added = popFilter.add("SecondTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 1);

        cleanupRedis(filterName);
    }

    @Test
    public void duplicateTest() {
        String filterName = "duplicateTest";

        cleanupRedis(filterName);
        BloomFilterRedis<String> popFilter = BloomFilterRedis.createPopulationFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 1);

        added = popFilter.add("FirstTest");
        assertTrue(!added);
        assertTrue(popFilter.getPopulation() == 1);

        added = popFilter.add("Secondtest");
        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 2);

        cleanupRedis(filterName);
    }

    @Test
    public void clearTest() {
        String filterName = "clearTest";

        cleanupRedis(filterName);

        BloomFilterRedis<String> popFilter = BloomFilterRedis.createPopulationFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        popFilter.add("FirstTest");
        popFilter.add("FirstTest");
        popFilter.add("SecondTest");
        popFilter.add("third");
        popFilter.add("fourth");
        popFilter.add("fifth");
        popFilter.add("sixth");

        popFilter.clear();
        assertTrue(popFilter.getPopulation() == 0);

        cleanupRedis(filterName);
    }

    @Test
    public void addAllTestNoPopulation() {

        String filterName = "addAllTest";

        cleanupRedis(filterName);

        BloomFilterRedis<String> popFilter = BloomFilterRedis.createFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == -1);

        List<String> toAdd = new ArrayList<>();
        toAdd.add("SecondTest");
        toAdd.add("ThirdTest");

        popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == -1);

        toAdd.clear();
        toAdd.add("FourthTest");
        toAdd.add("FifthTest");
        List<Boolean> wereAdded = popFilter.addAll(toAdd);

        for (boolean wasAdded : wereAdded) {
            assertTrue(wasAdded);
        }
        assertTrue(popFilter.getPopulation() == -1);

        toAdd.clear();
        toAdd.add("FifthTest");
        toAdd.add("SixthTest");

        wereAdded = popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == -1);

        assertTrue(!wereAdded.get(0));
        assertTrue(wereAdded.get(1));

        // do it again, should not change
        wereAdded = popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == -1);

        assertTrue(!wereAdded.get(0));
        assertTrue(!wereAdded.get(1));

        cleanupRedis(filterName);

    }

    @Test
    public void addAllTestWithPopulation() {

        String filterName = "addAllTest";

        cleanupRedis(filterName);

        BloomFilterRedis<String> popFilter = BloomFilterRedis.createPopulationFilter(getConnection(), filterName, defaultElements, defaultError, BloomFilter.HashMethod.Murmur);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 1);

        List<String> toAdd = new ArrayList<>();
        toAdd.add("SecondTest");
        toAdd.add("ThirdTest");

        popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == 3);

        toAdd.clear();
        toAdd.add("FourthTest");
        toAdd.add("FifthTest");
        List<Boolean> wereAdded = popFilter.addAll(toAdd);

        for (boolean wasAdded : wereAdded) {
            assertTrue(wasAdded);
        }
        assertTrue(popFilter.getPopulation() == 5);

        toAdd.clear();
        toAdd.add("FifthTest");
        toAdd.add("SixthTest");

        wereAdded = popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == 6);

        assertTrue(!wereAdded.get(0));
        assertTrue(wereAdded.get(1));

        // do it again, should not change
        wereAdded = popFilter.addAll(toAdd);
        assertTrue(popFilter.getPopulation() == 6);

        assertTrue(!wereAdded.get(0));
        assertTrue(!wereAdded.get(1));

        cleanupRedis(filterName);

    }

    public void cleanupRedis(String a_filterName) {
        Jedis jedis = getConnection();
        jedis.del(a_filterName);
        jedis.del(BloomFilterRedis.buildConfigKeyName(a_filterName));
        jedis.del(BloomFilterRedis.buildPopulationKeyName(a_filterName));
    }

    private Jedis getConnection() {
        String hostname = "localhost";
        int port = 6379;
        return new Jedis(hostname, port);
    }
}
