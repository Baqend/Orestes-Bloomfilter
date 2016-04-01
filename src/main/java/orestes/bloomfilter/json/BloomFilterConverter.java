package orestes.bloomfilter.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.BitSet;

public class BloomFilterConverter {

    /**
     * Converts a normal or Counting Bloom filter to a JSON representation of a non-counting Bloom filter.
     *
     * @param source the Bloom filter to convert
     * @return the JSON representation of the Bloom filter
     */
    public static JsonElement toJson(BloomFilter<?> source) {
        JsonObject root = new JsonObject();
        root.addProperty("m", source.getSize());
        root.addProperty("h", source.getHashes());
        //root.addProperty("HashMethod", source.config().hashMethod().name());
        byte[] bits = source.getBitSet().toByteArray();

        // Encode using Arrays.toString -> [0,16,0,0,32].
        // root.addProperty("bits", Arrays.toString(bits));

        // Encode using base64 -> AAAAAQAAQAAAAAAgA
        root.addProperty("b", toBase64(bits));

        return root;
    }

    /**
     * Converts a normal or Counting Bloom filter to a Base64 encoded string containing its bits.
     *
     * @param source the Bloom filter to convert
     * @return the Base64 representation of the Bloom filter
     */
    public static String toBase64(BloomFilter<?> source) {
        return toBase64(source.getBitSet().toByteArray());
    }

    private static String toBase64(byte[] bits) {
        return new String(Base64.getEncoder().encode(bits), StandardCharsets.UTF_8);
    }

    /**
     * Constructs a Bloom filter from its JSON representation.
     *
     * @param source the the JSON source
     * @return the constructed Bloom filter
     */
    public static BloomFilter<String> fromJson(JsonElement source) {
        return fromJson(source, String.class);
    }

    /**
     * Constructs a Bloom filter from its JSON representation
     *
     * @param source the JSON source
     * @param type   The class of the generic type
     * @param <T>    Generic type parameter of the Bloom filter
     * @return the Bloom filter
     */
    public static <T> BloomFilter<T> fromJson(JsonElement source, Class<T> type) {
        JsonObject root = source.getAsJsonObject();
        int m = root.get("m").getAsInt();
        int k = root.get("h").getAsInt();
        //String hashMethod = root.get("HashMethod").getAsString();
        byte[] bits = Base64.getDecoder().decode(root.get("b").getAsString());

        FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3KirschMitzenmacher);

        BloomFilterMemory<T> filter = new BloomFilterMemory<>(builder.complete());
        filter.setBitSet(BitSet.valueOf(bits));

        return filter;
    }


}
