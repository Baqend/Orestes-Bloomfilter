package orestes.bloomfilter.test;

import com.google.gson.JsonElement;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.json.BloomFilterConverter;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class ConverterTest {
    @Test
    public void testCorrectJSON() throws Exception {
        BloomFilter<String> bf = new FilterBuilder().expectedElements(50).falsePositiveProbability(0.1).buildBloomFilter();
        bf.add("Ululu");
        JsonElement json = BloomFilterConverter.toJson(bf);
        BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
        assertTrue(otherBf.contains("Ululu"));
    }

    @Test
    public void testCorrectJSONfromCountingFilter() throws Exception {
        BloomFilter<String> bf = new FilterBuilder().expectedElements(50).falsePositiveProbability(0.1).buildCountingBloomFilter();
        bf.add("Ululu");
        JsonElement json = BloomFilterConverter.toJson(bf);
        BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
        assertTrue(otherBf.contains("Ululu"));
    }
}
