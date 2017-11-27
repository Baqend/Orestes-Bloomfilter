package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.*;

public abstract class AbstractExpiringBloomFilterRedis<T> extends CountingBloomFilterRedis<T> implements ExpiringBloomFilter<T> {
    private final Clock clock;

    // Load the "report read" Lua script
    private final String reportReadScript = loadLuaScript("reportRead.lua");

    AbstractExpiringBloomFilterRedis(FilterBuilder builder) {
        super(builder);

        this.clock = pool.getClock();
    }

    @Override
    public boolean isCached(T element) {
        Long remaining = getRemainingTTL(element, MICROSECONDS);
        return remaining != null && remaining > 0;
    }

    @Override
    public Long getRemainingTTL(T element, TimeUnit unit) {
        return pool.safelyReturn(jedis -> {
            final Double score = jedis.zscore(keys.TTL_KEY, element.toString());
            return scoreToRemainingTTL(score, unit);
        });
    }

    @Override
    public List<Long> getRemainingTTLs(List<T> elements, TimeUnit unit) {
        return pool.safelyReturn((jedis) -> {
            // Retrieve scores from Redis
            final Pipeline pipe = jedis.pipelined();
            elements.forEach(it -> pipe.zscore(keys.TTL_KEY, it.toString()));
            final List<Object> scores = pipe.syncAndReturnAll();

            // Convert to desired time
            return scores
                    .stream()
                    .map(score -> (Double) score)
                    .map(score -> scoreToRemainingTTL(score, unit))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public void reportRead(T element, long TTL, TimeUnit unit) {
        pool.safelyDo((jedis) -> {
            // Create timestamp from TTL
            final double timestamp = remainingTTLToScore(TTL, unit);
            jedis.evalsha(reportReadScript, 1, keys.TTL_KEY, String.valueOf(timestamp), element.toString());
        });
    }

    @Override
    public Long reportWrite(T element, TimeUnit unit) {
        Long remaining = getRemainingTTL(element, unit);
        if (remaining == null || remaining <= 0) {
            return null;
        }

        add(element);
        addToQueue(element, remaining, unit);
        return remaining;
    }

    abstract void addToQueue(T element, long remaining, TimeUnit timeUnit);

    @Override
    public List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        List<Long> remainingTTLs = getRemainingTTLs(elements, MICROSECONDS);
        List<T> filteredElements = new LinkedList<>();
        List<Long> reportedTTLs = new LinkedList<>();
        for (int i = 0; i < remainingTTLs.size(); i++) {
            Long remaining = remainingTTLs.get(i);
            if (remaining == null || remaining < 0) {
                reportedTTLs.add(null);
                continue;
            }

            reportedTTLs.add(unit.convert(remaining, MICROSECONDS));

            T element = elements.get(i);
            filteredElements.add(element);
            addToQueue(element, remaining, MICROSECONDS);
        }
        addAll(filteredElements);
        return reportedTTLs;
    }

    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return toMemoryFilter();
    }

    @Override
    public void migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and binary BF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        final ExpiringBloomFilter<T> ebfSource = (ExpiringBloomFilter<T>) source;
        ebfSource.disableExpiration();

        CompletableFuture.allOf(
            // Migrate TTL list
            CompletableFuture.runAsync(() -> setTimeToLiveMap(ebfSource.getTimeToLiveMap())),

            // Migrate queue
            CompletableFuture.runAsync(() -> setExpirationMap(ebfSource.getExpirationMap()))
        ).join();

        ebfSource.enableExpiration();
    }

    /**
     * @return current timestamp in milliseconds
     */
    protected long now() {
        return clock.millis();
    }

    /**
     * Converts a desired unit to the score stored in Redis.
     *
     * @param TTL  the TTL to convert
     * @param unit the unit of the TTL
     * @return timestamp from TTL in microseconds
     */
    protected double remainingTTLToScore(long TTL, TimeUnit unit) {
        return clock.instant().plusMillis(MILLISECONDS.convert(TTL, unit)).toEpochMilli();
    }

    /**
     * Converts the score stored in Redis to a desired unit.
     *
     * @param score The score stored in Redis.
     * @param unit  The desired time unit.
     * @return The remaining TTL.
     */
    protected Long scoreToRemainingTTL(Double score, TimeUnit unit) {
        if (score == null) {
            return null;
        }

        final long sourceDuration = score.longValue() - now();
        final long convert = unit.convert(sourceDuration, MILLISECONDS);
        return convert <= 0 ? null : convert;
    }

    @Override
    public TimeMap<T> getTimeToLiveMap() {
        return pool.safelyReturn((jedis) -> {
            final Set<Tuple> tuples = jedis.zrangeByScoreWithScores(keys.TTL_KEY, now(), Double.POSITIVE_INFINITY);
            return tuples.stream().collect(TimeMap.collectMillis(t -> (T) t.getElement(), t -> (long) t.getScore()));
        });
    }

    @Override
    public void setTimeToLiveMap(TimeMap<T> map) {
        pool.safelyDo((jedis) -> {
            final Pipeline pipeline = jedis.pipelined();
            final AtomicInteger ctr = new AtomicInteger(0);
            map.forEach((item, ttl) -> {
                pipeline.zadd(keys.TTL_KEY, ttl, item.toString());
                // Sync every thousandth item
                if (ctr.incrementAndGet() >= 1000) {
                    ctr.set(0);
                    pipeline.sync();
                }
            });
            pipeline.sync();
        });
    }
}
