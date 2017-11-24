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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
        migrateExpirations(ebfSource.streamExpirations());

        // Migrate expiration queue
        migrateWrittenItems(ebfSource.streamWrittenItems());

        ebfSource.enableExpiration();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpirations() {
        return expirations.entrySet().stream().map(entry -> new ExpiringItem<>(entry.getKey(), toRelativeMicroseconds(entry.getValue())));
    }

    @Override
    public Stream<ExpiringItem<T>> streamWrittenItems() {
        return queue.streamEntries().map(item -> item.convert(NANOSECONDS, MICROSECONDS));
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

    private void migrateExpirations(Stream<ExpiringItem<T>> stream) {
        stream.forEach(item -> {
            final ExpiringItem<T> absolute = item.convert(MICROSECONDS, NANOSECONDS).addDelay(now());
            expirations.put(absolute.getItem(), absolute.getExpiration());
        });
    }

    private void migrateWrittenItems(Stream<ExpiringItem<T>> stream) {
        stream.map(it -> it.convert(MICROSECONDS, NANOSECONDS).addDelay(now())).forEach(queue::add);
    }

    /**
     * Converts the in-memory expirations to relative microseconds.
     *
     * @param nanosFromMemory The expirations to convert.
     * @return Relative microsecond representation of that value.
     */
    private long toRelativeMicroseconds(long nanosFromMemory) {
        return MICROSECONDS.convert(nanosFromMemory - now(), NANOSECONDS);
    }
}
