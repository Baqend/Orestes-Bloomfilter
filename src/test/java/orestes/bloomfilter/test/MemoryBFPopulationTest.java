package orestes.bloomfilter.test;

import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.BloomFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static orestes.bloomfilter.test.helper.Helper.createFilter;
import static org.junit.Assert.assertTrue;

public class MemoryBFPopulationTest {
    private int defaultK = 10;
    private int defaultM = 1000000;
    private HashMethod defaultFunction = HashMethod.MD5;

    private int defaultElements = 1_000_000;
    private double defaultError = 0.01;


    @Test
    public void emptyBF() {

        BloomFilter<String> popFilter = createFilter(defaultM, defaultK, defaultFunction);

        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 0);
    }

    @Test
    public void addTest() {
        BloomFilter<String> popFilter = createFilter(defaultM, defaultK, defaultFunction);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

    }

    @Test
    public void duplicateTest() {
        BloomFilter<String> popFilter = createFilter(defaultElements, defaultError, defaultFunction);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        added = popFilter.add("FirstTest");
        assertTrue(!added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 1);

        added = popFilter.add("Secondtest");
        assertTrue(added);
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 2);
    }

    @Test
    public void clearTest() {
        BloomFilter<String> popFilter = createFilter(defaultElements, defaultError, defaultFunction);
        popFilter.add("FirstTest");
        popFilter.add("FirstTest");
        popFilter.add("SecondTest");
        popFilter.add("third");
        popFilter.add("fourth");
        popFilter.add("fifth");
        popFilter.add("sixth");

        popFilter.clear();
        assertTrue(Math.round(popFilter.getEstimatedPopulation()) == 0);

    }

    @Test
    public void addAllTest() {
        BloomFilter<String> popFilter = createFilter(defaultElements, defaultError, defaultFunction);
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

    }

}
