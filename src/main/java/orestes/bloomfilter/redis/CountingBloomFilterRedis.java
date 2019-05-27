package orestes.bloomfilter.redis;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.MigratableBloomFilter;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Uses regular key-value pairs for counting instead of a bitarray. This introduces a space overhead but allows
 * distribution of keys, thus increasing throughput. Pipelining can also be leveraged in this approach to minimize
 * network latency.
 *
 * @param <T> The type of the containing elements
 */
public class CountingBloomFilterRedis<T> implements CountingBloomFilter<T>, MigratableBloomFilter<T> {
    protected final RedisKeys keys;
    protected final RedisPool pool;
    protected final RedisBitSet bloom;
    protected final FilterBuilder config;


    public CountingBloomFilterRedis(FilterBuilder builder) {
        FilterBuilder updateBuilder = builder.clone();
        builder.complete();

        this.keys = new RedisKeys(builder.name());
        this.pool = builder.pool();
        this.bloom = new RedisBitSet(pool, keys.BITS_KEY, builder.size());
        this.config = keys.persistConfig(pool, updateBuilder);
        if (builder.overwriteIfExists()) {
            this.clear();
        }
    }

    @Override
    public Map<Integer, Long> getCountMap() {
        try (Jedis r = pool.allowingSlaves().getResource()) {
            return RedisUtils.decodeMap(r.hgetAll(keys.COUNTS_KEY.getBytes()));
        }
    }

    @Override
    public long addAndEstimateCountRaw(byte[] element) {
        List<Object> results = pool.transactionallyRetry(p -> {
            int[] hashes = hash(element);
            for (int position : hashes) {
                bloom.set(p, position, true);
            }
            for (int position : hashes) {
                p.hincrBy(keys.COUNTS_KEY.getBytes(), RedisUtils.encodeKey(position), 1);
            }
        }, keys.BITS_KEY, keys.COUNTS_KEY);
        return results.stream().skip(config().hashes()).mapToLong(i -> (long) i).min().orElse(0L);
    }

    @Override
    public List<Boolean> addAll(Collection<T> elements) {
        return addAndEstimateCountRaw(elements).stream().map(el -> el == 1).collect(toList());
    }

    /**
     * Adds all elements to the Bloom filter and returns the estimated count of the inserted elements.
     *
     * @param elements The elements to add.
     * @return An estimation of how often the respective elements are in the Bloom filter.
     */
    private List<Long> addAndEstimateCountRaw(Collection<T> elements) {
        List<int[]> allHashes = elements.stream().map(el -> hash(toBytes(el))).collect(toList());
        List<Object> results = pool.transactionallyRetry(p -> {
            // Add to flattened Bloom filter
            for (int[] hashes : allHashes) {
                for (int position : hashes) {
                    bloom.set(p, position, true);
                }
            }
            // Add to counting Bloom filter
            for (int[] hashes : allHashes) {
                for (int position : hashes) {
                    p.hincrBy(keys.COUNTS_KEY.getBytes(), RedisUtils.encodeKey(position), 1);
                }
            }
        }, keys.BITS_KEY, keys.COUNTS_KEY);

        // Walk through result in blocks of #hashes and skip the return values from set-bit calls.
        // Get the minimum count for each added element to estimate how often the element is in the Bloom filter.
        List<Long> mins = new LinkedList<>();
        for (int i = results.size() / 2; i < results.size(); i += config().hashes()) {
            long min = results.subList(i, i + config().hashes())
                .stream()
                .map(val -> (Long) val)
                .min(Comparator.<Long>naturalOrder())
                .get();
            mins.add(min);
        }
        return mins;
    }

    //TODO removeALL


    @Override
    public boolean removeRaw(byte[] value) {
        return removeAndEstimateCountRaw(value) <= 0;
    }

    @Override
    public long removeAndEstimateCountRaw(byte[] value) {
        try (Jedis jedis = pool.getResource()) {
            while (true) {
                // Forbid writing to counting Bloom filter in the meantime
                jedis.watch(keys.COUNTS_KEY);

                // Get all Bloom filter positions to decrement when removing the element
                int[] positions = hash(value);

                // Get the resulting counts in the CBF after the elements have been removed
                Map<Integer, Long> posToCountMap = getCounts(jedis, positions);

                // Decrement counts
                HashMap<Integer, Integer> countMap = new HashMap<>(positions.length);
                for (int position : positions) {
                    countMap.compute(position, (k, v) -> (v == null) ? 1 : (v + 1));
                }
                posToCountMap.replaceAll((position, count) -> count - countMap.get(position));

                // Start updating the CBF and FBF transactionally
                Transaction tx = jedis.multi();

                // Decrement counts in CBF
                setCounts(tx, posToCountMap);

                // Reset bits in BBF
                updateBinaryBloomFilter(tx, posToCountMap);

                // Commit transaction and return whether successful
                boolean hasFailed = tx.exec().isEmpty();

                if (!hasFailed) {
                    //noinspection ConstantConditions
                    return posToCountMap.values().stream().mapToLong(Long::valueOf).min().getAsLong();
                }
            }
        }
    }

    @Override
    public long getEstimatedCount(T element) {
        try (Jedis jedis = pool.allowingSlaves().getResource()) {
            byte[][] hashesString = RedisUtils.encodeKey(hash(toBytes(element)));
            List<byte[]> hmget = jedis.hmget(keys.COUNTS_KEY.getBytes(), hashesString);
            return hmget.stream().mapToLong(i -> (i == null) ? 0L : RedisUtils.decodeValue(i)).min().orElse(0L);
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY);
        }
    }

    @Override
    public void remove() {
        clear();
        try (Jedis jedis = pool.getResource()) {
            jedis.del(config().name());
        }
        pool.destroy();
    }

    @Override
    public boolean contains(byte[] element) {
        return bloom.isAllSet(hash(element));
    }


    public RedisBitSet getRedisBitSet() {
        return bloom;
    }

    @Override
    public BitSet getBitSet() {
        return bloom.asBitSet();
    }

    public byte[] getBytes() {
        return bloom.toByteArray();
    }

    @Override
    public FilterBuilder config() {
        return config;
    }

    public CountingBloomFilterMemory<T> toMemoryFilter() {
        CountingBloomFilterMemory<T> filter = new CountingBloomFilterMemory<>(config().clone());
        filter.getBloomFilter().setBitSet(getBitSet());
        return filter;
    }

    @Override
    public CountingBloomFilter<T> clone() {
        return new CountingBloomFilterRedis<>(config().clone());
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

    public RedisPool getRedisPool() {
        return pool;
    }

    public RedisKeys getRedisKeys() {
        return keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CountingBloomFilterRedis)) {
            return false;
        }

        CountingBloomFilterRedis that = (CountingBloomFilterRedis) o;

        if (bloom != null ? !bloom.equals(that.bloom) : that.bloom != null) {
            return false;
        }
        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) {
            return false;
        }
        //TODO also checks counters

        return true;
    }

    @Override
    public void migrateFrom(BloomFilter<T> source) {
        if (!(source instanceof CountingBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        CountingBloomFilter<T> cbf = (CountingBloomFilter<T>) source;

        Map<Integer, Long> countSetToMigrate = cbf.getCountMap();

        pool.transactionallyRetry(p -> {
            countSetToMigrate.forEach((position, value) -> set(position, value, p));
        }, keys.BITS_KEY, keys.COUNTS_KEY);
    }

    /**
     * Get the counts in the CBF.
     *
     * @param jedis     A Jedis instance to use.
     * @param positions The positions to retrieve.
     * @return A map returning the count for each position.
     */
    private Map<Integer, Long> getCounts(Jedis jedis, int... positions) {
        // Get the corresponding keys for the positions in Redis
        byte[][] keysToDec = RedisUtils.encodeKey(positions);

        // Get the resulting counts in the CBF after the elements have been removed
        List<byte[]> values = jedis.hmget(keys.COUNTS_KEY.getBytes(), keysToDec);
        return IntStream.range(0, positions.length)
            .collect(
                HashMap::new,
                (m, i) -> m.put(positions[i], (values.get(i) == null) ? 0L : RedisUtils.decodeValue(values.get(i))),
                Map::putAll
            );
    }

    /**
     * Sets the counts in the CBF at the given positions by a map.
     *
     * @param p      The Jedis pipeline to use.
     * @param counts The new counts to set.
     */
    private void setCounts(PipelineBase p, Map<Integer, Long> counts) {
        p.hmset(keys.COUNTS_KEY.getBytes(), RedisUtils.encodeMap(counts));
    }

    /**
     * Updates the binary Bloom filter by new counts.
     *
     * @param p      The Jedis pipeline to use.
     * @param counts The new counts to update the filter from.
     */
    private void updateBinaryBloomFilter(PipelineBase p, Map<Integer, Long> counts) {
        counts.forEach((position, count) -> bloom.set(p, position, count > 0));
    }

    /**
     * Sets the value at the given position.
     *
     * @param position The position in the Bloom filter
     * @param value    The value to set
     * @param p        The jedis pipeline to use
     */
    private void set(int position, long value, Pipeline p) {
        bloom.set(p, position, value > 0);
        p.hset(keys.COUNTS_KEY.getBytes(), RedisUtils.encodeKey(position), RedisUtils.encodeValue(value));
    }
}
