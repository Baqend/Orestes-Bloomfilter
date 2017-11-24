package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Created by erik on 09.10.17.
 */
public class ExpiringBloomFilterPureRedis extends ExpiringBloomFilterRedis<String> {
    private final ExpirationQueueRedis redisQueue;
    private final String exportQueueScript = loadLuaScript("exportQueue.lua");

    /**
     * Logger for the {@link ExpiringBloomFilterPureRedis} class
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExpiringBloomFilterPureRedis.class);

    public ExpiringBloomFilterPureRedis(FilterBuilder builder) {
        super(builder, false);
        redisQueue = new ExpirationQueueRedis(builder, keys.EXPIRATION_QUEUE_KEY, this::expirationHandler);
        setQueue(redisQueue);
    }

    @Override
    public Long reportWrite(String element, TimeUnit unit) {
        Long ttl = super.reportWrite(element, unit);

        if (ttl != null) {
            redisQueue.triggerExpirationHandling(ttl, unit);
        }

        return ttl;
    }

    @Override
    public List<Long> reportWrites(List<String> elements, TimeUnit unit) {
        List<Long> ttls = super.reportWrites(elements, unit);

        long now = System.nanoTime();
        OptionalLong minTTL = ttls.stream()
                .filter(Objects::nonNull)
                .map(it -> TimeUnit.NANOSECONDS.convert(it, unit))
                .mapToLong(it -> it - now)
                .min();

        if (minTTL.isPresent()) {
            redisQueue.triggerExpirationHandling(minTTL.getAsLong(), TimeUnit.NANOSECONDS);
        }

        return ttls;
    }

    @Override
    public void clear() {
        pool.safelyDo((jedis) -> {
            // Delete all used fields from Redis
            jedis.del(keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY, keys.TTL_KEY);
        });
    }

    @Override
    public void remove() {
        super.remove();
        redisQueue.remove();
    }

    @Override
    public void migrateFrom(BloomFilter<String> source) {
        super.migrateFrom(source);
        redisQueue.triggerExpirationHandling(1, TimeUnit.NANOSECONDS);
    }

    @Override
    public Stream<ExpirationQueue.ExpiringItem<String>> streamWrittenItems() {
        return redisQueue.streamEntries();
    }

    /**
     * Handles expiring items from the expiration queue.
     *
     * @return true if successful, false otherwise.
     */
    synchronized public boolean expirationHandler() {
        final long now = now();
        LOG.debug("[" + config.name() + "] Expiring items ...");
        long expiredItems = pool.safelyReturn((jedis) -> (long) jedis.evalsha(
                exportQueueScript, 3,
                // Keys:
                keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY,
                // Args:
                String.valueOf(now)
        ));
        LOG.debug("[" + config.name() + "] Script expired " + expiredItems + " items within " + (now() - now) + "µs");
        return true;
    }

}
