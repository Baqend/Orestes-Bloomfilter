package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.redis.MessagePackEncoder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created on 06.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class MessagePackEncoderTest {

    private FilterBuilder builder;
    private MessagePackEncoder encoder;

    @Before
    public void setUp() throws Exception {
        builder = new FilterBuilder(10_000, 0.002).complete();
        encoder = new MessagePackEncoder();
    }

    @Test
    public void testEncodeItem() throws Exception {
        String name = "Some random name";

        int[] positions = builder.hashFunction().hash(name.getBytes(), builder.size(), builder.hashes());
        byte[] write = encoder.encodeItem(name, positions);
        printBytes(write);

        // Check pack is a three entry map
        assertEquals((byte) 0x83, write[0]);

        // Name at first position
        assertEquals((byte) 0xa4, write[1]);

        // Positions at second position
        assertEquals((byte) 0xa9, write[23]);

        // Check valid hash at third position
        assertEquals((byte) 0xa4, write[write.length - MessagePackEncoder.HASH_LENGTH - 6]);
        assertEquals((byte) 0xa8, write[write.length - MessagePackEncoder.HASH_LENGTH - 1]);
    }

    @Test
    public void testDecodeItem() throws Exception {
        byte[] read = {-125, -92, 110, 97, 109, 101, -80, 83, 111, 109, 101, 32, 114, 97, 110, 100, 111, 109, 32,
                110, 97, 109, 101, -87, 112, 111, 115, 105, 116, 105, 111, 110, 115, -103, -51, -1, 23, -51, -104, -122,
                -51, 49, -11, -50, 0, 1, -60, -87, -50, 0, 1, 94, 24, -51, -9, -121, -51, -112, -10, -51, 42, 101, -50,
                0, 1, -67, 25, -92, 104, 97, 115, 104, -88, 101, 100, 97, 49, 99, 57, 53, 56};

        assertEquals("Some random name", encoder.decodeItem(read));
    }

    void printBytes(byte[] bytes) {
        for (byte b : bytes) {
            char ch = (char) b;
            if (Character.isLetterOrDigit(b) || b == 32) {
                System.out.print(ch);
            } else {
                System.out.print("\\x" + Integer.toHexString(Byte.toUnsignedInt(b)));
            }
        }
        System.out.println();
    }
}
