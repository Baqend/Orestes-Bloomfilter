package orestes.bloomfilter.redis;

import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.PipelineBase;
import redis.clients.util.SafeEncoder;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * A persistent BitSet backed by Redis. Not all methods of the superclass are implemented. If needed they can be used
 * converting the RedisBitSet to a regular BitSet by calling {@link #asBitSet()}. <br> <br> External transactions or
 * pipeline can be propagated for use by modifying methods (e.g. {@link #set(int)}).
 */
public class RedisBitSet extends BitSet {
    private final RedisPool pool;
    private String name;
    private int size;

    /**
     * Constructs a new RedisBitSet.
     *
     * @param pool the redis connection pool
     * @param name the name used as key in the database
     * @param size the initial size of the RedisBitSet
     */
    public RedisBitSet(RedisPool pool, String name, int size) {
        this.pool = pool;
        this.name = name;
        this.size = size;
    }


    @Override
    public boolean get(int bitIndex) {
        return pool.allowingSlaves().safelyReturn(jedis -> jedis.getbit(name, bitIndex));
    }

    /**
     * Fetches the values at the given index positions in a multi transaction. This guarantees a consistent view.
     *
     * @param indexes the index positions to query
     * @return an array containing the values at the given index positions
     */
    public Boolean[] getBulk(int... indexes) {
        List<Boolean> results = pool.allowingSlaves().transactionallyDo(p -> {
            for (int index : indexes) {
                get(p, index);
            }
        });
        return results.toArray(new Boolean[indexes.length]);
    }

    @Override
    public void set(int bitIndex, boolean value) {
        pool.safelyDo(jedis -> jedis.setbit(name, bitIndex, value));
    }


    public void get(PipelineBase p, int position) {
        p.getbit(name, position);
    }

    /**
     * Performs the normal {@link #set(int, boolean)} operation using the given pipeline.
     *
     * @param p        the propagated pipeline
     * @param bitIndex a bit index
     * @param value    a boolean value to set
     */
    public void set(PipelineBase p, int bitIndex, boolean value) {
        p.setbit(name, bitIndex, value);
    }

    @Override
    public void set(int bitIndex) {
        set(bitIndex, true);
    }

    @Override
    public void clear(int bitIndex) {
        set(bitIndex, false);
    }

    public void clear(PipelineBase p, int bitIndex) {
        set(p, bitIndex, false);
    }

    @Override
    public void clear() {
        pool.safelyDo(jedis -> {
            jedis.del(name);
        });
    }

    @Override
    public int cardinality() {
        return pool.safelyReturn(jedis -> jedis.bitcount(name)).intValue();
    }


    @Override
    public int size() {
        return size;
    }


    @Override
    public byte[] toByteArray() {
        return pool.allowingSlaves().safelyReturn(jedis -> {
            byte[] bytes = jedis.get(SafeEncoder.encode(name));
            if (bytes == null) {
                //prevent null values
                bytes = new byte[(int) Math.ceil(size / 8)];
            }
            return bytes;
        });
    }

    @Override
    public boolean isEmpty() {
        return pool.safelyReturn(jedis -> jedis.bitcount(name) == 0);
    }

    @Override
    public int length() {
        return pool.safelyReturn(jedis -> {
            int byteCount = jedis.strlen(name).intValue();
            if (byteCount == 0) {
                return 0;
            }

            byte lastByte = jedis.getrange(name.getBytes(), byteCount - 1, byteCount)[0];
            int i = 8;
            while ((lastByte & 1) == 0) {
                i -= 1;
                lastByte >>>= 1;
            }
            return (8 * byteCount) - 8 + i;
        });
    }

    /**
     * Returns the RedisBitSet as a regular BitSet.
     *
     * @return this RedisBitSet as a regular BitSet
     */
    public BitSet asBitSet() {
        return fromByteArrayReverse(toByteArray());
    }


    /**
     * Overwrite the contents of this RedisBitSet by the given BitSet.
     *
     * @param bits a regular BitSet used to overwrite this RedisBitSet
     */
    public void overwriteBitSet(BitSet bits) {
        pool.safelyDo(jedis -> jedis.set(SafeEncoder.encode(name), toByteArrayReverse(bits)));
    }

    @Override
    public String toString() {
        return asBitSet().toString();
    }

    public String getRedisKey() {
        return name;
    }

    /**
     * Tests whether the provided bit positions are all set.
     *
     * @param positions the positions to test
     * @return <tt>true</tt> if all positions are set
     */
    public boolean isAllSet(int... positions) {
        Boolean[] results = getBulk(positions);
        return Stream.of(results).allMatch(b -> b);
    }

    /**
     * Set all bits
     *
     * @param positions The positions to set
     * @return {@code true} if any of the bits was previously unset.
     */
    public boolean setAll(int... positions) {
        List<Object> results = pool.transactionallyDo(p -> {
            for (int position : positions)
                p.setbit(name, position, true);
        });
        return results.stream().anyMatch(b -> !(Boolean) b);
    }

    //Copied from: https://github.com/xetorthio/jedis/issues/301
    public static BitSet fromByteArrayReverse(byte[] bytes) {
        BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[i / 8] & (1 << (7 - (i % 8)))) != 0) {
                bits.set(i);
            }
        }
        return bits;
    }

    //Copied from: https://github.com/xetorthio/jedis/issues/301
    public static byte[] toByteArrayReverse(BitSet bits) {
        byte[] bytes = new byte[(bits.length() + 7) / 8];
        for (int i = 0; i < bits.length(); i++) {
            if (bits.get(i)) {
                int value = bytes[i / 8] | (1 << (7 - (i % 8)));
                bytes[i / 8] = (byte) value;
            }
        }
        return bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RedisBitSet)
            obj = ((RedisBitSet) obj).asBitSet();
        return asBitSet().equals(obj);
    }
}
