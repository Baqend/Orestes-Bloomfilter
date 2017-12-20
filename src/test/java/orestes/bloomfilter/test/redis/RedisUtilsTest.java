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
        int i = 42;
        assertArrayEquals(new byte[] { 0, 0, 0, 42 }, RedisUtils.encodeKey(i));
        assertArrayEquals("\0\0\0*".getBytes(), RedisUtils.encodeKey(i));
        int j = 0x41424344;
        assertArrayEquals("ABCD".getBytes(), RedisUtils.encodeKey(j));
    }

    @Test
    public void testEncodeValue() {
        long i = 42;
        assertArrayEquals("42".getBytes(), RedisUtils.encodeValue(i));
        long j = 1337;
        assertArrayEquals("1337".getBytes(), RedisUtils.encodeValue(j));
    }

    @Test
    public void testEncodeKeyIsCollisionFree() {
        Random random = new Random();
        for (long x = 0; x <= 1_000_000; x += 1) {
            int i = Math.abs(random.nextInt());
            int j = Math.abs(random.nextInt());
            if (i == j) continue;

            byte[] str1 = RedisUtils.encodeKey(i);
            byte[] str2 = RedisUtils.encodeKey(j);

            assertFalse(i + " and " + j + " should differ", Arrays.equals(str1, str2));
        }
    }

    @Test
    public void testEncodeDecodeKeyIsSame() {
        Random random = new Random();
        for (long x = 0; x <= 1_000_000; x += 1) {
            int i = Math.abs(random.nextInt());
            assertEquals(i, RedisUtils.decodeKey(RedisUtils.encodeKey(i)));
        }
    }
}
