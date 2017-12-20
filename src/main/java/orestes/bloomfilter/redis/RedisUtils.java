package orestes.bloomfilter.redis;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

/**
 * Created on 22.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class RedisUtils {
    private RedisUtils() {
    }

    /**
     * Encodes a map as byte arrays.
     *
     * @param map The map to encode.
     * @return A map of byte arrays.
     */
    public static Map<byte[], byte[]> encodeMap(Map<Integer, Long> map) {
        return map.entrySet().stream()
            .collect(toMap(e -> encodeKey(e.getKey()), e -> encodeValue(e.getValue())));
    }

    /**
     * Encodes an integer key as byte array.
     *
     * @param key The key to encode.
     * @return A byte array representing that key.
     */
    public static byte[] encodeKey(int key) {
        return ByteBuffer.allocate(4).putInt(key).array();
    }

    /**
     * Encodes many integers as byte arrays.
     *
     * @param keys The keys to encode.
     * @return An array of byte arrays representing those keys.
     */
    public static byte[][] encodeKey(int[] keys) {
        return IntStream.of(keys).mapToObj(RedisUtils::encodeKey).toArray(byte[][]::new);
    }

    /**
     * Encodes an integer value as byte array.
     *
     * @param value The value to encodeKey.
     * @return A byte array representing that value.
     */
    public static byte[] encodeValue(long value) {
        return String.valueOf(value).getBytes();
    }

    /**
     * Decodes a map from byte arrays.
     *
     * @param map The map to decode.
     * @return A decoded map from byte arrays.
     */
    public static Map<Integer, Long> decodeMap(Map<byte[], byte[]> map) {
        return map.entrySet().stream()
            .collect(toMap(e -> decodeKey(e.getKey()), e -> decodeValue(e.getValue())));
    }

    /**
     * Decodes an integer key which is wrapped in a byte array.
     *
     * @param key The byte array to decode.
     * @return A key decoded from that byte array.
     */
    public static int decodeKey(byte[] key) {
        return ByteBuffer.wrap(key).getInt();
    }

    /**
     * Decodes an integer value which is wrapped in a byte array.
     *
     * @param value The byte array to decode.
     * @return A value decoded from that byte array.
     */
    public static long decodeValue(byte[] value) {
        return Long.parseLong(new String(value));
    }
}
