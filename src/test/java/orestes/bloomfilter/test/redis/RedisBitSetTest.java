package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.redis.RedisBitSet;
import orestes.bloomfilter.redis.helper.RedisPool;
import orestes.bloomfilter.test.helper.Helper;
import org.junit.Test;

import java.util.BitSet;
import java.util.Random;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class RedisBitSetTest {

    @Test
    public void testBitSetStoredCorrectly() {
        int length = 20;
        BitSet primes = new BitSet(length);
        primes.flip(2, length);
        for (int i = 0; i < length; i++) {
            if (primes.get(i)) {
                for (int j = i * 2; j < length; j += i) {
                    primes.set(j, false);
                }
            }
        }

        RedisPool pool = Helper.getPool();
        RedisBitSet bs = new RedisBitSet(pool, "test", primes.size());
        bs.overwriteBitSet(primes);
        BitSet read = bs.asBitSet();
        assertEquals(primes, read);
        assertEquals(primes, read);
    }

    @Test
    public void testEquals() {
        BitSet b1 = new BitSet();
        BitSet b2 = new BitSet();
        IntStream.range(0, 100).forEach(i -> b1.set(i*10, true));
        IntStream.range(0, 100).forEach(i -> b2.set(i*99, true));

        RedisPool pool = Helper.getPool();
        RedisBitSet rb1 = new RedisBitSet(pool, "b1", b1.size());
        rb1.overwriteBitSet(b1);
        RedisBitSet rb2 = new RedisBitSet(pool, "b2", b2.size());
        rb2.overwriteBitSet(b2);

        assertEquals(b1, rb1.asBitSet());
        assertEquals(b2, rb2.asBitSet());
        assertThat(rb1, not(equalTo(rb2)));
        assertThat(b1, not(equalTo(b2)));
        assertThat(rb1, not(equalTo(b2)));
    }

    @Test
    public void testCardinality() {
        int max = 1_000_000;
        BitSet b1 = new BitSet();
        RedisBitSet b2 = new RedisBitSet(Helper.getPool(), "card", max);
        b2.clear();

        new Random().ints(1000).forEach(i -> {
            int index = Math.abs(i % max);
            b1.set(index, true);
            b2.set(index, true);
        });

        assertEquals(b1.cardinality(), b2.cardinality());
    }

    @Test
    public void testIsEmpty() throws Exception {
        int max = 1_000_000;
        RedisBitSet bitSet = new RedisBitSet(Helper.getPool(), "isEmpty", max);
        bitSet.clear();
        assertTrue(bitSet.isEmpty());

        bitSet.set(0, true);
        assertFalse(bitSet.isEmpty());
        bitSet.set(0, false);
        assertTrue(bitSet.isEmpty());

        bitSet.set(0, true);
        bitSet.set(1, true);
        assertFalse(bitSet.isEmpty());
        bitSet.set(0, false);
        assertFalse(bitSet.isEmpty());

        bitSet.clear();
        assertTrue(bitSet.isEmpty());
    }

    @Test
    public void testLength() throws Exception {
        int max = 1_000_000;
        RedisBitSet bitSet = new RedisBitSet(Helper.getPool(), "length", max);

        bitSet.clear();
        assertEquals(0, bitSet.length());

        bitSet.set(0, true);
        assertEquals(1, bitSet.length());

        bitSet.set(1, true);
        assertEquals(2, bitSet.length());

        bitSet.set(7, true);
        assertEquals(8, bitSet.length());
        bitSet.set(8, true);
        assertEquals(9, bitSet.length());

        bitSet.set(31, true);
        assertEquals(32, bitSet.length());
        bitSet.set(32, true);
        assertEquals(33, bitSet.length());

        bitSet.set(63, true);
        assertEquals(64, bitSet.length());
        bitSet.set(64, true);
        assertEquals(65, bitSet.length());

        bitSet.clear();
        assertEquals(0, bitSet.length());
    }

    @Test
    public void testFromByteArrayReverse() throws Exception {
        byte[] bytes = {-128, 0, 0, 1};
        BitSet bitSet = RedisBitSet.fromByteArrayReverse(bytes);
        assertEquals(32, bitSet.length());
        assertTrue(bitSet.get(0));
        for (int i = 1; i < 31; i += 1) {
            assertFalse(bitSet.get(i));
        }
        assertTrue(bitSet.get(31));

        // Check reverse operation
        byte[] reverse = RedisBitSet.toByteArrayReverse(bitSet);
        assertArrayEquals(bytes, reverse);
    }

    @Test
    public void testToByteArrayReverse() throws Exception {
        BitSet bitSet = new BitSet();
        bitSet.set(0, true);
        bitSet.set(15, true);
        bitSet.set(24, true);
        assertEquals(25, bitSet.length());

        byte[] bytes = RedisBitSet.toByteArrayReverse(bitSet);
        byte[] expected = {-128, 1, 0, -128};
        assertArrayEquals(expected, bytes);

        // Check reverse operation
        BitSet reverse = RedisBitSet.fromByteArrayReverse(bytes);
        assertEquals(bitSet, reverse);
    }
}
