package orestes.bloomfilter.redis;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisKeys;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.SafeEncoder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Uses regular key-value pairs for counting instead of a bitarray. This introduces a space overhead but allows
 * distribution of keys, thus increasing throughput. Pipelining can also be leveraged in this approach to minimize
 * network latency.
 *
 * @param <T> The type of the containing elements
 */
public class CountingBloomFilterRedis<T> implements CountingBloomFilter<T> {
    protected final RedisKeys keys;
    protected final RedisPool pool;
    protected final RedisBitSet bloom;
    protected final FilterBuilder config;


    public CountingBloomFilterRedis(FilterBuilder builder) {
        builder.complete();
        this.keys = new RedisKeys(builder.name());
        this.pool = builder.pool();
        this.bloom = new RedisBitSet(pool, keys.BITS_KEY, builder.size());
        this.config = keys.persistConfig(pool, builder);
        if (builder.overwriteIfExists()) {
            this.clear();
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
                p.hincrBy(keys.COUNTS_KEY, encode(position), 1);
            }
        }, keys.BITS_KEY, keys.COUNTS_KEY);
        return results.stream().skip(config().hashes()).map(i -> (Long) i).min(Comparator.<Long>naturalOrder()).get();
    }

    @Override
    public List<Boolean> addAll(Collection<T> elements) {
        return addAndEstimateCountRaw(elements).stream().map(el -> el == 1).collect(Collectors.toList());
    }

    private List<Long> addAndEstimateCountRaw(Collection<T> elements) {
        List<int[]> allHashes = elements.stream().map(el -> hash(toBytes(el))).collect(Collectors.toList());
        List<Object> results = pool.transactionallyRetry(p -> {
            for (int[] hashes : allHashes) {
                for (int position : hashes) {
                    bloom.set(p, position, true);
                }
            }
            for (int[] hashes : allHashes) {
                for (int position : hashes) {
                    p.hincrBy(keys.COUNTS_KEY, encode(position), 1);
                }
            }
        }, keys.BITS_KEY, keys.COUNTS_KEY);

        //Walk through result in blocks of #hashes and skip the return values from set-bit calls
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
        return pool.safelyReturn(jedis -> {
            int[] hashes = hash(value);
            String[] hashesString = encode(hashes);

            Pipeline p = jedis.pipelined();
            p.watch(keys.COUNTS_KEY, keys.BITS_KEY);

            List<Long> counts;
            List<Response<Long>> responses = new ArrayList<>(config().hashes());
            for (String position : hashesString) {
                responses.add(p.hincrBy(keys.COUNTS_KEY, position, -1));
            }
            p.sync();
            counts = responses.stream().map(Response::get).collect(Collectors.toList());

            //Fast lane: don't set BF bits
            long min = Collections.min(counts);
            if (min > 0) {
                return min;
            }

            while (true) {
                p = jedis.pipelined();
                p.multi();
                for (int i = 0; i < config().hashes(); i++) {
                    if (counts.get(i) <= 0) {
                        bloom.set(p, hashes[i], false);
                    }
                }
                Response<List<Object>> exec = p.exec();
                p.sync();
                if (exec.get() == null) {
                    p = jedis.pipelined();
                    p.watch(keys.COUNTS_KEY, keys.BITS_KEY);
                    Response<List<String>> hmget = p.hmget(keys.COUNTS_KEY, hashesString);
                    p.sync();
                    counts = hmget.get()
                        .stream()
                        .map(o -> o != null ? Long.parseLong(o) : 0l)
                        .collect(Collectors.toList());
                } else {
                    return Collections.min(counts);
                }
            }
        });
    }

    @Override
    public long getEstimatedCount(T element) {
        return pool.allowingSlaves().safelyReturn(jedis -> {
            String[] hashesString = encode(hash(toBytes(element)));
            List<String> hmget = jedis.hmget(keys.COUNTS_KEY, hashesString);
            return hmget.stream().mapToLong(i -> i == null ? 0L : Long.valueOf(i)).min().orElse(0L);
        });
    }


    @Override
    public void clear() {
        pool.safelyDo(jedis -> {
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY);
        });
    }

    @Override
    public void remove() {
        clear();
        pool.safelyDo(jedis -> jedis.del(config().name()));
        pool.destroy();
    }

    @Override
    public boolean contains(byte[] element) {
        return bloom.isAllSet(hash(element));
    }


    protected RedisBitSet getRedisBitSet() {
        return bloom;
    }

    @Override
    public BitSet getBitSet() {
        return bloom.asBitSet();
    }

    public byte[] getBytes() { return bloom.toByteArray(); }

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

    private static String encode(int value) {
        return SafeEncoder.encode(
            new byte[]{(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value});
    }


    private static String[] encode(int[] hashes) {
        return IntStream.of(hashes).mapToObj(CountingBloomFilterRedis::encode).toArray(String[]::new);
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

}
