package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilter32;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ExpiringBloomFilterMemory<T> extends CountingBloomFilter32<T> implements ExpiringBloomFilter<T> {
    private final Map<T, Long> expirations = new ConcurrentHashMap<>();
    private ExpirationQueue<T> queue;

    public ExpiringBloomFilterMemory(FilterBuilder config) {
        super(config);
        this.queue = new ExpirationQueueMemory<>(this::onExpire);
    }

    private Long ttlToTimestamp(long TTL, TimeUnit unit) {
        return System.nanoTime() + TimeUnit.NANOSECONDS.convert(TTL, unit);
    }

    private void onExpire(ExpiringItem<T> entry) {
        this.remove(entry.getItem());
        expirations.remove(entry.getItem(), entry.getExpiration());
    }

    @Override
    public boolean isCached(T element) {
        return expirations.containsKey(element);
    }

    @Override
    public Long getRemainingTTL(T element, TimeUnit unit) {
        Long ts = expirations.get(element);
        return ts != null ? unit.convert(ts - System.nanoTime(), TimeUnit.NANOSECONDS) : null;
    }

    @Override
    public synchronized void reportRead(T element, long TTL, TimeUnit unit) {
        long ts = ttlToTimestamp(TTL, unit);
        expirations.compute(element, (k, v) -> v == null ? ts : Math.max(ts, v));
    }

    @Override
    public synchronized Long reportWrite(T element, TimeUnit unit) {
        //Only add if there is a potentially cached read
        Long ts = expirations.get(element);
        if (ts != null) {
            add(element);
            queue.addExpiration(element, ts);
        }
        return ts != null ? unit.convert(ts - System.nanoTime(), TimeUnit.NANOSECONDS) : null;
    }

    @Override
    public Double getEstimatedPopulation() {
        return (double) expirations.size();
    }

    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return filter.clone();
    }

    @Override
    public void clear() {
        super.clear();
        if (queue != null) {
            queue.clear();
        }
        expirations.clear();
    }
}
