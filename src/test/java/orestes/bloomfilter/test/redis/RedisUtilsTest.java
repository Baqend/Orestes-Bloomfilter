package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.redis.RedisUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created on 22.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class RedisUtilsTest {

    @Test
    public void testEncodeKey() {
        final int i = 42;
        assertArrayEquals(new byte[] { 0, 0, 0, 42 }, RedisUtils.encodeKey(i));
        final int j = 0x41424344;
        assertArrayEquals("ABCD".getBytes(), RedisUtils.encodeKey(j));
    }

    @Test
    public void testEncodeValue() {
        final long i = 42;
        assertArrayEquals("42".getBytes(), RedisUtils.encodeValue(i));
        final long j = 1337;
        assertArrayEquals("1337".getBytes(), RedisUtils.encodeValue(j));
    }

    @Test
    public void testEncodeKeyIsCollisionFree() {
        final Random random = new Random();
        for (long x = 0; x <= 1_000_000; x += 1) {
            final int i = Math.abs(random.nextInt());
            final int j = Math.abs(random.nextInt());
            if (i == j) continue;

            final byte[] str1 = RedisUtils.encodeKey(i);
            final byte[] str2 = RedisUtils.encodeKey(j);

            assertFalse(i + " and " + j + " should differ", Arrays.equals(str1, str2));
        }
    }

    @Test
    public void testEncodeDecodeKeyIsSame() {
        final Random random = new Random();
        for (long x = 0; x <= 1_000_000; x += 1) {
            final int i = Math.abs(random.nextInt());
            assertEquals(i, RedisUtils.decodeKey(RedisUtils.encodeKey(i)));
        }
    }
}
