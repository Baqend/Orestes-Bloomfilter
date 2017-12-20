package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.MigratableBloomFilter;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilter32;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ExpiringBloomFilterMemory<T> extends CountingBloomFilter32<T> implements ExpiringBloomFilter<T>, MigratableBloomFilter<T> {
    private final TimeMap<T> ttlMap = new TimeMap<>();
    private final ExpirationQueue<T> queue;

    public ExpiringBloomFilterMemory(FilterBuilder config) {
        super(config);
        this.queue = new ExpirationQueueMemory<>(this::onExpire);
    }

    private void onExpire(ExpiringItem<T> entry) {
        this.remove(entry.getItem());
        ttlMap.remove(entry.getItem(), entry.getExpiration(MILLISECONDS));
    }

    @Override
    public synchronized void reportRead(T element, long TTL, TimeUnit unit) {
        ttlMap.putRemaining(element, TTL, unit);
    }

    @Override
    public synchronized Long reportWrite(T element, TimeUnit unit) {
        // Only add if there is a potentially cached read
        Long expiration = ttlMap.get(element);
        if (expiration == null) {
            return null;
        }

        add(element);
        queue.addExpiration(element, expiration, TimeUnit.MILLISECONDS);
        return unit.convert(expiration - ttlMap.now(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Double getEstimatedPopulation() {
        return (double) ttlMap.size();
    }

    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return filter.clone();
    }

    @Override
    public void clear() {
        super.clear();
        queue.clear();
        ttlMap.clear();
    }

    @Override
    public void migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and FBF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        ExpiringBloomFilter<T> ebfSource = (ExpiringBloomFilter<T>) source;
        ebfSource.disableExpiration();

        // Migrate TTL map
        setTimeToLiveMap(ebfSource.getTimeToLiveMap());

        // Migrate expiration queue
        setExpirationMap(ebfSource.getExpirationMap());

        ebfSource.enableExpiration();
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

    @Override
    public TimeMap<T> getTimeToLiveMap() {
        return ttlMap;
    }

    @Override
    public void setTimeToLiveMap(TimeMap<T> map) {
        ttlMap.putAll(map);
    }
}
