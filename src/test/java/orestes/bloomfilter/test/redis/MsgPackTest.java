package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue;
import orestes.bloomfilter.cachesketch.ExpirationQueueRedis;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created on 06.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class MsgPackTest {

    private FilterBuilder builder;
    private ExpirationQueueRedis queue;

    @Before
    public void setUp() throws Exception {
        builder = new FilterBuilder(10_000, 0.002).complete();
        queue = new ExpirationQueueRedis(builder, "foo", () -> true);
    }

    @Test
    public void testEncodeItem() throws Exception {
        final String name = "Some random name";

        final byte[] write = queue.encodeItem(new ExpirationQueue.ExpiringItem<>(name, 0), "demo");
        printBytes(write);

        assertEquals((byte) 0x83, write[0]);
        assertEquals((byte) 0xa4, write[1]); // Name at first position
        assertEquals((byte) 0xa9, write[23]); // Positions at second position
        assertEquals((byte) 0xa4, write[67]); // Hash at third position
    }

    void printBytes(byte[] bytes) {
        for (byte b : bytes) {
            final char ch = (char) b;
            if (Character.isLetterOrDigit(b) || b == 32) {
                System.out.print(ch);
            } else {
                System.out.print("\\x" + Integer.toHexString(Byte.toUnsignedInt(b)));
            }
        }
        System.out.println();
    }
}
