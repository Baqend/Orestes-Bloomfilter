package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import org.junit.Test;

import java.util.stream.IntStream;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ExpiringTest {

    private long fromMillis(long millis) {
        return millis * 1_000_000;
    }

    @Test
    public void addAndLetExpire() throws Exception {
        FilterBuilder b = new FilterBuilder(1000, 0.05);
        ExpiringBloomFilter<String> filter = new ExpiringBloomFilter<String>(b);
        filter.reportRead("1", fromMillis(100));
        assertTrue(filter.isCached("1"));
        assertFalse(filter.contains("1"));
        filter.reportWrite("1");
        assertTrue(filter.contains("1"));
        Thread.sleep(110);
        assertFalse(filter.contains("1"));
    }

    @Test
    public void exceedCapacity() {
        FilterBuilder b = new FilterBuilder(100, 0.05);
        ExpiringBloomFilter<String> filter = new ExpiringBloomFilter<>(b);

        IntStream.range(0, 200).forEach(i -> {
            filter.reportRead(String.valueOf(i), fromMillis(1000));
            filter.reportWrite(String.valueOf(i));
            //System.out.println(filter.getEstimatedPopulation() + ":" + filter.getEstimatedFalsePositiveProbability());
        });
        //System.out.println(filter.getFalsePositiveProbability(200));

        //fpp exceeded
        assertTrue(filter.getEstimatedFalsePositiveProbability() > 0.05);
        //Less then 10% difference between estimated and precise fpp
        assertTrue(Math.abs(1-filter.getEstimatedFalsePositiveProbability()/filter.getFalsePositiveProbability(200)) < 0.1);
    }

}
