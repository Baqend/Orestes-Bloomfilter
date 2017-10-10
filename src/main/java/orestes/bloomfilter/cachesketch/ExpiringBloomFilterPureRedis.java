package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
            .filter(it -> it != null)
            .map(it -> TimeUnit.NANOSECONDS.convert(it, unit))
            .mapToLong(it -> it - now)
            .min();

        if (minTTL.isPresent()) {
            redisQueue.triggerExpirationHandling(minTTL.getAsLong(), TimeUnit.NANOSECONDS);
        }

        return ttls;
    }

    /**
     * Handles expiring items from the expiration queue.
     *
     * @param queue The queue that may have expired items.
     *
     * @return true if successful, false otherwise.
     */
    private boolean expirationHandler(ExpirationQueueRedis queue) {
        return pool.safelyReturn(p -> {
            p.watch(keys.COUNTS_KEY, keys.EXPIRATION_QUEUE_KEY);

            final List<String> uniqueQueueKeys = queue.getExpiredItems(p);
            final List<String> expiredElems = uniqueQueueKeys.stream().map(queue::normalize).collect(toList());

            // If no element is expired, we have nothing to do
            if (expiredElems.isEmpty()) {
                return true;
            }

            // Get all Bloom filter positions to decrement when removing the elements
            final List<Integer> positionsToDec = expiredElems.stream()
                .map(this::hash)
                .flatMapToInt(IntStream::of)
                .boxed()
                .collect(toList());

            // Get the corresponding keys for the positions in Redis
            final List<String> keysToDec = positionsToDec.stream().map(String::valueOf).collect(toList());


            // Get the resulting counts in the CBF after the elements have been removed
            List<Long> keyCountAfterDec = p.hmget(keys.COUNTS_KEY, keysToDec.toArray(new String[keysToDec.size()]))
                .stream()
                .map(s -> s == null? "0" : s)
                .map(Long::valueOf)
                .map(l -> l - 1)
                .collect(toList());

            // Get the positions to reset in the flat Bloom filter (FBF)
            List<Integer> positionsToReset = positionsToDec.stream()
                .filter(position -> keyCountAfterDec.remove(0) <= 0)
                .collect(toList());

            // Start updating the queue, CBF and FBF transactionally
            Transaction t = p.multi();

            // Remove expired elements from queue
            queue.removeElements(uniqueQueueKeys, t);

            // Decrement counts in CBF
            keysToDec.forEach(pos -> t.hincrBy(keys.COUNTS_KEY, pos, -1));
            // Reset bits in FBF
            positionsToReset.forEach(pos -> bloom.set(t, pos, false));

            // Commit transaction and return whether successful
            List<Object> commit = t.exec();
            return commit != null;
        });
    }
}
