package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

public class ExpiringBloomFilterRedis<T> extends AbstractExpiringBloomFilterRedis<T> {
    private final ExpirationQueue<T> queue;

    public ExpiringBloomFilterRedis(FilterBuilder builder) {
        super(builder);

        // Init expiration queue which removes elements from Bloom filter if entry expires
        this.queue = new ExpirationQueueMemory<>(this::onExpire);
    }

    @Override
    protected void addToQueue(T element, long remaining, TimeUnit timeUnit) {
        queue.addExpiration(element, now() + remaining, timeUnit);
    }

    @Override
    public void clear() {
        try (Jedis jedis = pool.getResource()) {
            // Clear CBF, Bits, and TTLs
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY, keys.TTL_KEY);
            clearExpirationQueue();
        }
    }

    @Override
    public void softClear() {
        try (Jedis jedis = pool.getResource()) {
            // Clear CBF, Bits, and TTLs
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY);
            clearExpirationQueue();
        }
    }

    private void clearExpirationQueue() {
        if (queue != null) {
            queue.clear();
        }
    }

    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return toMemoryFilter();
    }

    @Override
    public boolean setExpirationEnabled(boolean enabled) {
        return queue.setEnabled(enabled);
    }

    @Override
    public TimeMap<T> getExpirationMap() {
        return queue.getExpirationMap();
    }

    @Override
    public void setExpirationMap(TimeMap<T> map) {
        queue.setExpirationMap(map);
    }

    /**
     * Handler for the expiration queue, removes entry from Bloom filter
     *
     * @param entry The entry which expired in the ExpirationQueue
     */
    private void onExpire(ExpiringItem<T> entry) {
        this.removeAndEstimateCount(entry.getItem());
    }
}
