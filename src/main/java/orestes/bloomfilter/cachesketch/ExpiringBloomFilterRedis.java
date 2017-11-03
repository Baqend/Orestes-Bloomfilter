package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.Pipeline;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    /**
     * @param TTL  the TTL to convert
     * @param unit the unit of the TTL
     * @return timestamp from TTL in milliseconds
     */
    private long ttlToTimestamp(long TTL, TimeUnit unit) {
        return clock.instant().plusMillis(TimeUnit.MILLISECONDS.convert(TTL, unit)).toEpochMilli();
    }

    @Override
    public boolean isCached(T element) {
        Long remaining = getRemainingTTL(element, TimeUnit.MILLISECONDS);
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
        // Create timestamp from TTL
        final long timestamp = ttlToTimestamp(TTL, unit);

        pool.safelyDo(jedis -> jedis.evalsha(reportReadScript, 1, keys.TTL_KEY, String.valueOf(timestamp), element.toString()));
    }

    @Override
    public Long reportWrite(T element, TimeUnit unit) {
        Long remaining = getRemainingTTL(element, TimeUnit.NANOSECONDS);
        if (remaining != null && remaining >= 0) {
            add(element);
            queue.addTTL(element, remaining);
        }
        return remaining != null ? unit.convert(remaining, TimeUnit.NANOSECONDS) : null;
    }

    @Override
    public List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        List<Long> remainingTTLs = getRemainingTTLs(elements, TimeUnit.NANOSECONDS);
        List<T> filteredElements = new LinkedList<>();
        List<Long> reportedTTLs = new LinkedList<>();
        for (int i = 0; i < remainingTTLs.size(); i++) {
            Long remaining = remainingTTLs.get(i);
            if (remaining != null && remaining >= 0) {
                reportedTTLs.add(unit.convert(remaining, TimeUnit.NANOSECONDS));

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
        // TODO Refactor TTL list to use a redis map before implementing this.
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpiringBFItems() {
        return queue.streamEntries();
    }

    @Override
    public CountingBloomFilterRedis<T> migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and FBF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        // TODO migrate TLL list
        ((ExpiringBloomFilter<T>) source).streamExpirations();

        // migrate queue
        ((ExpiringBloomFilter<T>) source).streamExpiringBFItems().forEach(queue::add);

        return this;
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

        final long convert = unit.convert(score.longValue() - now(), TimeUnit.MILLISECONDS);
        return convert <= 0 ? null : convert;
    }

    /**
     * @return current timestamp in milliseconds
     */
    private long now() {
        return clock.instant().toEpochMilli();
    }
}
