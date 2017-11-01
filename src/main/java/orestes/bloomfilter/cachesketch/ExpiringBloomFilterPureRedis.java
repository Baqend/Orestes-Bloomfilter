package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by erik on 09.10.17.
 */
public class ExpiringBloomFilterPureRedis extends ExpiringBloomFilterRedis<String> {
    private final ExpirationQueueRedis redisQueue;

    public ExpiringBloomFilterPureRedis(FilterBuilder builder) {
        super(builder, false);

        redisQueue = new ExpirationQueueRedis(pool, keys.EXPIRATION_QUEUE_KEY, this::expirationHandler);
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
            jedis.del(keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY);

            clearTTLs(jedis);
        });
    }

    /**
     * Handles expiring items from the expiration queue.
     *
     * @param queue The queue that may have expired items.
     * @return true if successful, false otherwise.
     */
    synchronized private boolean expirationHandler(ExpirationQueueRedis queue) {
        return pool.safelyReturn((jedis) -> {
            // Forbid writing to expiration queue, CBF and FBF in the meantime
            jedis.watch(keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY);

            // Get expired elements
            final Set<String> uniqueQueueKeys = queue.getExpiredItems(jedis);
            final List<String> expiredElements = uniqueQueueKeys.stream().map(queue::normalize).collect(toList());

            // If no element is expired, we have nothing to do
            if (expiredElements.isEmpty()) {
                return true;
            }

            // Get all Bloom filter positions to decrement when removing the elements
            final int[] positions = expiredElements.stream()
                    .map(this::hash)
                    .flatMapToInt(IntStream::of)
                    .toArray();

            // Get the resulting counts in the CBF after the elements have been removed
            final Map<Integer, Long> posToCountMap = getCounts(jedis, positions);

            // Decrement counts
            final HashMap<Integer, Integer> countMap = new HashMap<>(positions.length);
            for (int position : positions) {
                countMap.compute(position, (k, v) -> v == null ? 1 : v + 1);
            }
            posToCountMap.replaceAll((position, count) -> count - countMap.get(position));

            // Start updating the queue, CBF and FBF transactionally
            final Transaction tx = jedis.multi();

            // Remove expired elements from queue
            queue.removeElements(uniqueQueueKeys, tx);

            // Decrement counts in CBF
            setCounts(tx, posToCountMap);

            // Reset bits in BBF
            updateBinaryBloomFilter(tx, posToCountMap);

            // Commit transaction and return whether successful
            boolean isAborted = tx.exec().isEmpty();

            return !isAborted;
        });
    }

    @Override
    public CountingBloomFilterRedis<String> migrateFrom(BloomFilter<String> source) {
        return super.migrateFrom(source);
    }
}
