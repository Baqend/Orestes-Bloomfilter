package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Tracks a mapping from objects to expirations and a Bloom filter of objects that are automatically removed after
 * expiration(obj).
 *
 */
public class ExpiringBloomFilterMemory<T> extends CountingBloomFilterMemory<T> implements ExpiringBloomFilter<T> {
    private final Map<T, Long> expirations = new ConcurrentHashMap<>();
    private ExpirationQueue<T> queue;

    public ExpiringBloomFilterMemory(FilterBuilder config) {
        super(config);
        this.queue = new ExpirationQueue<>(this::onExpire);
    }

    private Long ttlToTimestamp(long TTL, TimeUnit unit) {
        return System.nanoTime() + TimeUnit.NANOSECONDS.convert(TTL, unit);
    }

    private void onExpire(ExpiringItem<T> entry) {
        expirations.remove(entry.getItem(), entry.getExpiration());
        this.remove(entry.getItem());
    }

    @Override
    public boolean isCached(T element) {
        return expirations.containsKey(element);
    }

    @Override
    public Long getRemainingTTL(T element, TimeUnit unit) {
        Long remaining = expirations.get(element);
        return remaining != null ? unit.convert(remaining, TimeUnit.NANOSECONDS) : null;
    }

    @Override
    public synchronized void reportRead(T element, long TTL, TimeUnit unit) {
        long ts = ttlToTimestamp(TTL, unit);
        expirations.computeIfPresent(element, (k, v) -> Math.max(ts, v));
        expirations.putIfAbsent(element, ts);
    }

    @Override
    public synchronized Long reportWrite(T element, TimeUnit unit) {
        //Only add if there is a potentially cached read
        Long remaining = expirations.get(element);
        if (remaining != null) {
            add(element);
            queue.add(element, remaining);
        }
        return remaining != null ? unit.convert(remaining, TimeUnit.NANOSECONDS) : null;
    }

    public BloomFilter<T> getClonedBloomFilter() {
        return filter.clone();
    }


}
