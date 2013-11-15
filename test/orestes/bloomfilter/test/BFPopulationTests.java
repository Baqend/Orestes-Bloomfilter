package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BFPopulationTests {
    private int defaultM = 10;
    private int defaultK = 1000000;
    private String defaultFunction = "MD5";

    private double defaultElements = 1_000_000;
    private double defaultError = 0.01;

    @Test
    public void emptyBF() {

        BloomFilter<String> popFilter = createFilter(defaultM, defaultK, defaultFunction);

        assertTrue(popFilter.getPopulation() == 0);
    }

    @Test
    public void addTest() {
        BloomFilter<String> popFilter = createFilter(defaultM, defaultK, defaultFunction);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 1);

    }

    @Test
    public void duplicateTest() {
        BloomFilter<String> popFilter = createFilter(defaultElements, defaultError, defaultFunction);
        boolean added = popFilter.add("FirstTest");

        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 1);

        added = popFilter.add("FirstTest");
        assertTrue(!added);
        assertTrue(popFilter.getPopulation() == 1);

        added = popFilter.add("Secondtest");
        assertTrue(added);
        assertTrue(popFilter.getPopulation() == 2);
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
        assertTrue(popFilter.getPopulation() == 0);

    }

    @Test
    public void addAllTest() {
        BloomFilter<String> popFilter = createFilter(defaultElements, defaultError, defaultFunction);
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

    }


    private BloomFilter<String> createFilter(int m, int k, String hash) {
        BloomFilter<String> filter = new BloomFilter<>(m, k);
        filter.setCryptographicHashFunction(hash);
        return filter;
    }

    private BloomFilter<String> createFilter(double n, double p, String hash) {
        BloomFilter<String> filter = new BloomFilter<>(n, p);
        filter.setCryptographicHashFunction(hash);
        return filter;
    }
}
