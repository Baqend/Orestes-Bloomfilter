package orestes.bloomfilter.redis;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

/**
 * A persistent bloom filter backed by the Redis key value store. Internally it operates on the <i>setbit</i> and
 * <i>getbit</i> operations of Redis. If you need to remove elements from the bloom filter, please use a counting bloom
 * filter, e.g. {@link CountingBloomFilterRedis}. The performance of this data structure is very good, as operations are
 * grouped into fast transactions, minimizing the network overhead of all bloom filter operations to one round trip to
 * Redis.
 *
 * @param <T> The type of the containing elements
 */
public class BloomFilterRedis<T> implements BloomFilter<T> {
    private final RedisPool pool;
    private final RedisBitSet bloom;
    private final FilterBuilder config;

    public BloomFilterRedis(FilterBuilder builder) {
        FilterBuilder updateBuilder = builder.clone();
        builder.complete();

        RedisKeys keys = new RedisKeys(builder.name());
        this.pool = builder.pool();
        this.bloom = new RedisBitSet(pool, keys.BITS_KEY, builder.size());
        this.config = keys.persistConfig(pool, updateBuilder);
        if (builder.overwriteIfExists())
            this.clear();
    }

    @Override
    public FilterBuilder config() {
        return config;
    }

    @Override
    public boolean addRaw(byte[] element) {
        return bloom.setAll(hash(element));
    }

    @Override
    public List<Boolean> addAll(Collection<T> elements) {
        List<Boolean> added = new ArrayList<>();
        List<Boolean> results = pool.transactionallyDo(p -> {
            for (T value : elements) {
                for (int position : hash(toBytes(value))) {
                    bloom.set(p, position, true);
                }
            }
        });

        //For each value check, if any bits were set to one
        boolean wasAdded = false;
        int numProcessed = 0;
        for (Boolean item : results) {
            if (!item) wasAdded = true;
            if ((numProcessed + 1) % config().hashes() == 0) {
                added.add(wasAdded);
                wasAdded = false;
            }
            numProcessed++;
        }
        return added;
    }

    public List<Boolean> contains(Collection<T> elements) {
        List<Boolean> contains = new ArrayList<>();
        List<Boolean> results = pool.transactionallyDo(p -> {
            for (T value : elements) {
                for (int position : hash(toBytes(value))) {
                    bloom.get(p, position);
                }
            }
        });

        //For each value check, if all bits in ranges of #hashes bits are set
        boolean isPresent = true;
        int numProcessed = 0;
        for (Boolean item : results) {
            if (!item) isPresent = false;
            if ((numProcessed + 1) % config().hashes() == 0) {
                contains.add(isPresent);
                isPresent = true;
            }
            numProcessed++;
        }
        return contains;
    }

    @Override
    public boolean contains(byte[] element) {
        return bloom.isAllSet(hash(element));
    }

    @Override
    public void clear() {
        bloom.clear();
    }

    @Override
    public void remove() {
        clear();
        pool.safelyDo(jedis -> jedis.del(config().name()));
        pool.destroy();
    }

    @Override
    public BitSet getBitSet() {
        return bloom.asBitSet();
    }

    public BloomFilterMemory<T> toMemoryFilter() {
        BloomFilterMemory<T> filter = new BloomFilterMemory<>(config().clone());
        filter.setBitSet(getBitSet());
        return filter;
    }

    @Override
    public BloomFilter<T> clone() {
        return new BloomFilterRedis<>(config.clone());
    }

    @Override
    public boolean union(BloomFilter<T> other) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean intersect(BloomFilter<T> other) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return bloom.isEmpty();
    }

    @Override
    public Double getEstimatedPopulation() {
        return BloomFilter.population(bloom, config());
    }

    /**
     * Returns the underlying RedisBitSet implementation of this RedisBloomFilter
     * @return The underlying RedisBitSet
     */
    public RedisBitSet getRedisBitSet() {
        return bloom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BloomFilterRedis)) return false;

        BloomFilterRedis that = (BloomFilterRedis) o;

        if (bloom != null ? !bloom.equals(that.bloom) : that.bloom != null) return false;
        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) return false;

        return true;
    }


}
