package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class ExpiringTest {



    @Test
    public void addAndLetExpire() throws Exception {
        FilterBuilder b = new FilterBuilder(1000, 0.05);
        ExpiringBloomFilterMemory<String> filter = new ExpiringBloomFilterMemory<String>(b);
        filter.reportRead("1", 100, TimeUnit.MILLISECONDS);
        assertTrue(filter.isCached("1"));
        assertTrue(filter.getRemainingTTL("1", TimeUnit.MILLISECONDS) >= 0);
        assertFalse(filter.contains("1"));
        filter.reportWrite("1");
        assertTrue(filter.contains("1"));
        Thread.sleep(110);
        assertFalse(filter.contains("1"));
        assertEquals(null, filter.getRemainingTTL("1", TimeUnit.MILLISECONDS));
    }

    @Test
    public void exceedCapacity() {
        FilterBuilder b = new FilterBuilder(100, 0.05);
        ExpiringBloomFilterMemory<String> filter = new ExpiringBloomFilterMemory<>(b);

        IntStream.range(0, 200).forEach(i -> {
            filter.reportRead(String.valueOf(i), 1000, TimeUnit.MILLISECONDS);
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
