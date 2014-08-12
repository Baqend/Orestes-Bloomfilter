package orestes.bloomfilter.test;

import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.test.helper.Helper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static orestes.bloomfilter.test.helper.Helper.cleanupRedis;
import static org.junit.Assert.assertTrue;

public class RedisBFPopulationTest {

    private int defaultElements = 1_000;
    private double defaultError = 0.01;

    @Test
    public void emptyBF() {
        int defaultM = 5;
        int defaultK = 1000000;

        String filterName = "emptyTest";

        cleanupRedis();

        BloomFilter<String> popFilter =  Helper.createRedisFilter(filterName, defaultM, defaultK, HashMethod.Murmur2);

        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 0);

        cleanupRedis();
    }

    @Test
    public void addTest() {
        String filterName = "addTest";

        cleanupRedis();
        BloomFilter<String> popFilter = Helper.createRedisFilter(filterName, defaultElements, defaultError, HashMethod.Murmur2);
        boolean added = popFilter.add("SecondTest");

        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        cleanupRedis();
    }

    @Test
    public void duplicateTest() {
        String filterName = "duplicateTest";

        cleanupRedis();
        BloomFilter<String> popFilter = Helper.createRedisFilter(filterName, defaultElements, defaultError, HashMethod.Murmur2);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        added = popFilter.add("FirstTest");
        assertTrue(!added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        added = popFilter.add("Secondtest");
        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 2);

        cleanupRedis();
    }

    @Test
    public void clearTest() {
        String filterName = "clearTest";

        cleanupRedis();

        BloomFilter<String> popFilter = Helper.createRedisFilter(filterName, defaultElements, defaultError, HashMethod.Murmur2);
        popFilter.add("FirstTest");
        popFilter.add("FirstTest");
        popFilter.add("SecondTest");
        popFilter.add("third");
        popFilter.add("fourth");
        popFilter.add("fifth");
        popFilter.add("sixth");

        popFilter.clear();
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 0);

        cleanupRedis();
    }

    @Test
    public void addAllTestNoPopulation() {

        String filterName = "addAllTest";

        cleanupRedis();

        BloomFilter<String> popFilter = Helper.createRedisFilter(filterName, defaultElements, defaultError, HashMethod.Murmur2);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);

        List<String> toAdd = new ArrayList<>();
        toAdd.add("SecondTest");
        toAdd.add("ThirdTest");

        popFilter.addAll(toAdd);

        toAdd.clear();
        toAdd.add("FourthTest");
        toAdd.add("FifthTest");
        List<Boolean> wereAdded = popFilter.addAll(toAdd);

        for (boolean wasAdded : wereAdded) {
            assertTrue(wasAdded);
        }

        toAdd.clear();
        toAdd.add("FifthTest");
        toAdd.add("SixthTest");

        wereAdded = popFilter.addAll(toAdd);

        assertTrue(!wereAdded.get(0));
        assertTrue(wereAdded.get(1));

        // do it again, should not change
        wereAdded = popFilter.addAll(toAdd);

        assertTrue(!wereAdded.get(0));
        assertTrue(!wereAdded.get(1));

        cleanupRedis();

    }

    @Test
    public void addAllTestWithPopulation() {

        String filterName = "addAllTest";

        cleanupRedis();

        BloomFilter<String> popFilter = Helper.createRedisFilter(filterName, defaultElements, defaultError, HashMethod.Murmur2);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        List<String> toAdd = new ArrayList<>();
        toAdd.add("SecondTest");
        toAdd.add("ThirdTest");

        popFilter.addAll(toAdd);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 3);

        toAdd.clear();
        toAdd.add("FourthTest");
        toAdd.add("FifthTest");
        List<Boolean> wereAdded = popFilter.addAll(toAdd);

        for (boolean wasAdded : wereAdded) {
            assertTrue(wasAdded);
        }
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 5);

        toAdd.clear();
        toAdd.add("FifthTest");
        toAdd.add("SixthTest");

        wereAdded = popFilter.addAll(toAdd);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 6);

        assertTrue(!wereAdded.get(0));
        assertTrue(wereAdded.get(1));

        // do it again, should not change
        wereAdded = popFilter.addAll(toAdd);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 6);

        assertTrue(!wereAdded.get(0));
        assertTrue(!wereAdded.get(1));

        cleanupRedis();

    }



}
