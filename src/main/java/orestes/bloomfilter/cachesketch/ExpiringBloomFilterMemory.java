package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.MigratableBloomFilter;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilter32;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class ExpiringBloomFilterMemory<T> extends CountingBloomFilter32<T> implements ExpiringBloomFilter<T>, MigratableBloomFilter<T> {
    private final Map<T, Long> expirations = new ConcurrentHashMap<>();
    private ExpirationQueue<T> queue;

    public ExpiringBloomFilterMemory(FilterBuilder config) {
        super(config);
        this.queue = new ExpirationQueueMemory<>(this::onExpire);
    }

    private Long ttlToTimestamp(long TTL, TimeUnit unit) {
        return now() + TimeUnit.NANOSECONDS.convert(TTL, unit);
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
        return ts != null ? unit.convert(ts - now(), TimeUnit.NANOSECONDS) : null;
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
        return ts != null ? unit.convert(ts - now(), TimeUnit.NANOSECONDS) : null;
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
    public void migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and FBF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        final ExpiringBloomFilter<T> ebfSource = (ExpiringBloomFilter<T>) source;
        ebfSource.disableExpiration();

        // Migrate TTL list
        migrateExpirations(ebfSource);

        // Migrate expiration queue
        ebfSource.streamExpiringBFItems().forEach(queue::add);

        ebfSource.enableExpiration();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpirations() {
        return expirations.entrySet().stream().map(entry -> new ExpiringItem<>(entry.getKey(), entry.getValue() - now()));
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpiringBFItems() {
        return queue.streamEntries();
    }

    @Override
    public boolean setExpirationEnabled(boolean enabled) {
        return queue.setEnabled(enabled);
    }

    /**
     * Returns the current time in nanoseconds.
     *
     * @return the current time in nanoseconds.
     */
    private long now() {
        return System.nanoTime();
    }

    private void migrateExpirations(ExpiringBloomFilter<T> ebfSource) {
        ebfSource.streamExpirations()
                .forEach(item -> {
                    final ExpiringItem<T> absolute = item.toAbsolute(now(), TimeUnit.NANOSECONDS);
                    expirations.put(absolute.getItem(), absolute.getExpiration());
                });
    }
}
