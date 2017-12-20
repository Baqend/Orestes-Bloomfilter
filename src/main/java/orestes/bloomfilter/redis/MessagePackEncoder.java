package orestes.bloomfilter.redis;

import org.msgpack.MessagePack;
import org.msgpack.type.MapValue;
import org.msgpack.type.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created on 27.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class MessagePackEncoder {
    /**
     * The length of a random item hash to make it unique.
     */
    public static final int HASH_LENGTH = 8;

    private final MessagePack messagePack;

    public MessagePackEncoder() {
        this.messagePack = new MessagePack();
    }

    /**
     * Encodes the given item into a message pack.
     *
     * @param item The item to encodeKey.
     * @param positions Positions within a Bloom filter to encode.
     * @return A packed item.
     */
    public byte[] encodeItem(String item, int[] positions) {
        String hash = createRandomHash();
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", item);
        map.put("hash", hash);
        map.put("positions", positions);

        try {
            return messagePack.write(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decodes a given item from a message pack.
     *
     * @param bytes The bytes to decode.
     * @return An unpacked item name.
     */
    public String decodeItem(byte[] bytes) {
        MapValue map;
        try {
            map = messagePack.read(bytes).asMapValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            if (entry.getKey().asRawValue().getString().equals("name")) {
                return entry.getValue().asRawValue().getString();
            }
        }

        throw new RuntimeException("Name is missing in message pack");
    }

    private String createRandomHash() {
        return UUID.randomUUID().toString().substring(0, HASH_LENGTH);
    }
}
