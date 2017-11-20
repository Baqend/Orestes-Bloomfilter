package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by erik on 09.10.17.
 */
public class ExpiringBloomFilterPureRedis extends ExpiringBloomFilterRedis<String> {
    private final ExpirationQueueRedis redisQueue;
    private final int id =  new Random().nextInt();
    private final String exportQueueScript = loadLuaScript("exportQueue.lua");

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

    /**
     * Handles expiring items from the expiration queue.
     *
     * @param queue The queue that may have expired items.
     * @return true if successful, false otherwise.
     */
    synchronized private boolean expirationHandler(ExpirationQueueRedis queue) {
        return pool.safelyReturn((jedis) -> {
            final String now = String.valueOf(System.nanoTime());
            jedis.evalsha(exportQueueScript, 3, keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY, now);

            return true;
        });
    }
}
