package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilter32;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    @Override
    public CountingBloomFilter<T> migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and FBF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        // Migrate TTL list
        ((ExpiringBloomFilter<T>) source).streamExpirations()
            .forEach(item -> expirations.put(item.getItem(), item.getExpiration()));

        // Migrate expiration queue
        ((ExpiringBloomFilter<T>) source).streamExpiringBFItems().forEach(queue::add);

        return this;
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpirations() {
        return expirations.entrySet().stream().map(entry -> new ExpiringItem<T>(entry.getKey(), entry.getValue()));
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpiringBFItems() {
        return queue.streamEntries();
    }
}
