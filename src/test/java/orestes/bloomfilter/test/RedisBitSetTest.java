package orestes.bloomfilter.test;

import orestes.bloomfilter.redis.RedisBitSet;
import orestes.bloomfilter.redis.helper.RedisPool;
import orestes.bloomfilter.test.helper.Helper;
import org.junit.Test;

import java.util.BitSet;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class RedisBitSetTest {

    @Test
    public void testBitSetStoredCorrectly() {
        final int length = 20;
        final BitSet primes = new BitSet(length);
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


}
