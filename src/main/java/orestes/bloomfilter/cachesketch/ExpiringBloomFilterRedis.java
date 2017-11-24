package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.RedisUtils;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpiringBloomFilterRedis<T> extends CountingBloomFilterRedis<T> implements ExpiringBloomFilter<T> {
    private final Clock clock;
    private ExpirationQueue<T> queue;

    // Load the "report read" Lua script
    private final String reportReadScript = loadLuaScript("reportRead.lua");

    public ExpiringBloomFilterRedis(FilterBuilder builder) {
        this(builder, true);
    }

    ExpiringBloomFilterRedis(FilterBuilder builder, boolean initQueue) {
        super(builder);

        this.clock = pool.getClock();

        if (initQueue) {
            // Init expiration queue which removes elements from Bloom filter if entry expires
            this.queue = new ExpirationQueueMemory<>(this::onExpire);
        }

    }

    /**
     * Sets the given expiration queue.
     *
     * @param queue The expiration queue to set
     */
    public void setQueue(ExpirationQueue<T> queue) {
        if (this.queue != null) {
            throw new RuntimeException("You cannot set a queue if there is already one.");
        }
        this.queue = queue;
    }

    /**
     * Handler for the expiration queue, removes entry from Bloom filter
     *
     * @param entry The entry which expired in the ExpirationQueue
     */
    private void onExpire(ExpiringItem<T> entry) {
        this.removeAndEstimateCount(entry.getItem());
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
        Long remaining = getRemainingTTL(element, NANOSECONDS);
        if (remaining != null && remaining >= 0) {
            add(element);
            queue.addTTL(element, remaining);
        }
        return remaining != null ? unit.convert(remaining, NANOSECONDS) : null;
    }

    @Override
    public List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        List<Long> remainingTTLs = getRemainingTTLs(elements, MICROSECONDS);
        List<T> filteredElements = new LinkedList<>();
        List<Long> reportedTTLs = new LinkedList<>();
        for (int i = 0; i < remainingTTLs.size(); i++) {
            Long remaining = remainingTTLs.get(i);
            if (remaining != null && remaining >= 0) {
                reportedTTLs.add(unit.convert(remaining, MICROSECONDS));

                T element = elements.get(i);
                filteredElements.add(element);
                queue.addTTL(element, remaining);
            } else {
                reportedTTLs.add(null);
            }
        }
        addAll(filteredElements);
        return reportedTTLs;
    }

    @Override
    public void clear() {
        pool.safelyDo((jedis) -> {
            // Clear CBF, Bits, and TTLs
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY, keys.TTL_KEY);
            // During init, ONLY clear CBF
            if (queue == null) {
                return;
            }
            // Clear Queue
            queue.clear();
        });
    }

    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return toMemoryFilter();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpirations() {
        return pool.safelyReturn((jedis) -> {
            final Set<Tuple> tuples = jedis.zrangeByScoreWithScores(keys.TTL_KEY, now(), Double.POSITIVE_INFINITY);

            return tuples
                    .stream()
                    .map(it -> (ExpiringItem<T>) new ExpiringItem<>(it.getElement(), (long) it.getScore() - now()));
        });
    }

    @Override
    public Stream<ExpiringItem<T>> streamWrittenItems() {
        return queue.streamEntries().map(item -> item.convert(NANOSECONDS, MICROSECONDS));
    }

    @Override
    public boolean setExpirationEnabled(boolean enabled) {
        return queue.setEnabled(enabled);
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
            CompletableFuture.runAsync(() -> migrateExpirations(ebfSource.streamExpirations())),

            // Migrate queue
            CompletableFuture.runAsync(() -> queue.addMany(ebfSource.streamWrittenItems()))
        ).join();

        ebfSource.enableExpiration();
    }

    /**
     * @return current timestamp in microseconds
     */
    protected long now() {
        return RedisUtils.getRedisTimePoint(clock);
    }

    /**
     * Converts a desired unit to the score stored in Redis.
     *
     * @param TTL  the TTL to convert
     * @param unit the unit of the TTL
     * @return timestamp from TTL in microseconds
     */
    private double remainingTTLToScore(long TTL, TimeUnit unit) {
        return now() + MICROSECONDS.convert(TTL, unit);
    }

    /**
     * Converts the score stored in Redis to a desired unit.
     *
     * @param score The score stored in Redis.
     * @param unit  The desired time unit.
     * @return The remaining TTL.
     */
    private Long scoreToRemainingTTL(Double score, TimeUnit unit) {
        if (score == null) {
            return null;
        }

        final long convert = unit.convert(score.longValue() - now(), MICROSECONDS);
        return convert <= 0 ? null : convert;
    }

    /**
     * Streams items into the TTL list.
     *
     * @param items The stream of items.
     */
    private void migrateExpirations(Stream<ExpiringItem<T>> items) {
        pool.safelyDo((jedis) -> {
            final Pipeline pipeline = jedis.pipelined();
            final AtomicInteger ctr = new AtomicInteger(0);
            items.forEach((item) -> {
                pipeline.zadd(keys.TTL_KEY, remainingTTLToScore(item.getExpiration(), MICROSECONDS), item.getItem().toString());
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
