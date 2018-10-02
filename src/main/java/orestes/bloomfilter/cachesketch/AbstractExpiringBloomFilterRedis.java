package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractExpiringBloomFilterRedis<T> extends CountingBloomFilterRedis<T> implements ExpiringBloomFilter<T> {
    private final Clock clock;
    protected final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "BloomFilterExpiryThreadPool");
        thread.setDaemon(true);
        return thread;
    });
    // Load the "report read" Lua script
    private final String reportReadScript = loadLuaScript("reportRead.lua");

    protected AbstractExpiringBloomFilterRedis(FilterBuilder builder) {
        super(builder);

        this.clock = pool.getClock();
        // Schedule TTL map cleanup
        long interval = config.cleanupInterval();
        scheduler.scheduleAtFixedRate(this::cleanupTTLs, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isCached(T element) {
        Long remaining = getRemainingTTL(element, MICROSECONDS);
        return (remaining != null) && (remaining > 0);
    }

    @Override
    public Long getRemainingTTL(T element, TimeUnit unit) {
        try (Jedis jedis = pool.getResource()) {
            Double score = jedis.zscore(keys.TTL_KEY, element.toString());
            return scoreToRemainingTTL(score, unit);
        }
    }

    @Override
    public boolean isKnown(T element) {
        try (Jedis jedis = pool.getResource()) {
            Double score = jedis.zscore(keys.TTL_KEY, element.toString());
            if (score == null) {
                return  false;
            }

            long endOfGracePeriod = now() - config.gracePeriod();
            return score.longValue() > endOfGracePeriod;
        }
    }

    @Override
    public List<Boolean> isKnown(List<T> elements) {
        try (Jedis jedis = pool.getResource()) {
            // Retrieve scores from Redis
            Pipeline pipe = jedis.pipelined();
            elements.forEach(it -> pipe.zscore(keys.TTL_KEY, it.toString()));
            List<Object> scores = pipe.syncAndReturnAll();

            long endOfGracePeriod = now() - config.gracePeriod();
            // Convert to boolean
            return scores
                    .stream()
                    .map(score -> score != null && ((Double) score).longValue() > endOfGracePeriod)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<Long> getRemainingTTLs(List<T> elements, TimeUnit unit) {
        try (Jedis jedis = pool.getResource()) {
            // Retrieve scores from Redis
            Pipeline pipe = jedis.pipelined();
            elements.forEach(it -> pipe.zscore(keys.TTL_KEY, it.toString()));
            List<Object> scores = pipe.syncAndReturnAll();

            // Convert to desired time
            return scores
                .stream()
                .map(score -> (Double) score)
                .map(score -> scoreToRemainingTTL(score, unit))
                .collect(Collectors.toList());
        }
    }

    @Override
    public void reportRead(T element, long TTL, TimeUnit unit) {
        try (Jedis jedis = pool.getResource()) {
            // Create timestamp from TTL
            long timestamp = remainingTTLToScore(TTL, unit);
            jedis.evalsha(reportReadScript, 1, keys.TTL_KEY, String.valueOf(timestamp), element.toString());
        }
    }

    @Override
    public Long reportWrite(T element, TimeUnit unit) {
        Long remaining = getRemainingTTL(element, unit);
        if ((remaining == null) || (remaining <= 0)) {
            return null;
        }

        add(element);
        addToQueue(element, remaining, unit);
        return remaining;
    }

    @Override
    public List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        List<Long> remainingTTLs = getRemainingTTLs(elements, MICROSECONDS);
        List<T> filteredElements = new LinkedList<>();
        List<Long> reportedTTLs = new LinkedList<>();
        for (int i = 0; i < remainingTTLs.size(); i++) {
            Long remaining = remainingTTLs.get(i);
            if ((remaining == null) || (remaining < 0)) {
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
        // Check if other Bloom filter is compatible
        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        // Migrate CBF and binary BF
        super.migrateFrom(source);

        ExpiringBloomFilter<T> ebfSource = (ExpiringBloomFilter<T>) source;
        ebfSource.disableExpiration();

        CompletableFuture.allOf(
            // Migrate TTL list
            CompletableFuture.runAsync(() -> setTimeToLiveMap(ebfSource.getTimeToLiveMap())),

            // Migrate queue
            CompletableFuture.runAsync(() -> setExpirationMap(ebfSource.getExpirationMap()))
        ).join();

        ebfSource.enableExpiration();
    }

    @Override
    public TimeMap<T> getTimeToLiveMap() {
        try (Jedis jedis = pool.getResource()) {
            Set<Tuple> tuples = jedis.zrangeByScoreWithScores(keys.TTL_KEY, now() - config.gracePeriod(), Double.POSITIVE_INFINITY);
            return tuples.stream().collect(TimeMap.collectMillis(t -> (T) t.getElement(), t -> (long) t.getScore()));
        }
    }

    @Override
    public void setTimeToLiveMap(TimeMap<T> map) {
        try (Jedis jedis = pool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            AtomicInteger ctr = new AtomicInteger(0);
            map.forEach((item, ttl) -> {
                pipeline.zadd(keys.TTL_KEY, ttl, item.toString());
                // Sync every thousandth item
                if (ctr.incrementAndGet() >= 1000) {
                    ctr.set(0);
                    pipeline.sync();
                }
            });
            pipeline.sync();
        }
    }

    @Override
    public void cleanupTTLs() {
        try (Jedis jedis = pool.getResource()) {
            jedis.zremrangeByScore(keys.TTL_KEY, 0, now() - config.gracePeriod());
        }
    }

    /**
     * Add an element to this Bloom filter's expiration queue.
     *
     * @param element   The element to add.
     * @param remaining The remaining time.
     * @param timeUnit  The remaining time's unit.
     */
    protected abstract void addToQueue(T element, long remaining, TimeUnit timeUnit);

    /**
     * @return current timestamp in milliseconds
     */
    protected long now() {
        return clock.millis();
    }

    /**
     * Load a Lua script into Redis.
     *
     * @param filename The filename of the script.
     * @return A handle to the loaded script.
     */
    protected String loadLuaScript(String filename) {
        InputStream stream = AbstractExpiringBloomFilterRedis.class.getResourceAsStream(filename);
        String script = new BufferedReader(new InputStreamReader(stream)).lines().collect(Collectors.joining("\n"));
        return pool.safelyReturn(jedis -> jedis.scriptLoad(script));
    }

    /**
     * Converts a desired unit to the score stored in Redis.
     *
     * @param TTL  the TTL to convert
     * @param unit the unit of the TTL
     * @return timestamp from TTL in microseconds
     */
    private long remainingTTLToScore(long TTL, TimeUnit unit) {
        return clock.instant().plusMillis(MILLISECONDS.convert(TTL, unit)).toEpochMilli();
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

        long sourceDuration = score.longValue() - now();
        long convert = unit.convert(sourceDuration, MILLISECONDS);
        return (convert <= 0) ? null : convert;
    }
}
