package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.MigratableBloomFilter;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.memory.CountingBloomFilter32;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExpiringBloomFilterMemory<T> extends CountingBloomFilter32<T> implements ExpiringBloomFilter<T>, MigratableBloomFilter<T> {
    private final TimeMap<T> ttlMap = new TimeMap<>();
    private final ExpirationQueue<T> queue;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "BloomFilterCleanupThreadPool");
        thread.setDaemon(true);
        return thread;
    });

    public ExpiringBloomFilterMemory(FilterBuilder config) {
        super(config);
        this.queue = new ExpirationQueueMemory<>(this::onExpire);
        // Schedule TTL map cleanup
        long interval = config.cleanupInterval();
        scheduler.scheduleAtFixedRate(this::cleanupTTLs, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void onExpire(ExpiringItem<T> entry) {
        this.remove(entry.getItem());
    }

    @Override
    public void cleanupTTLs() {
        long now = ttlMap.now();
        for (T key : ttlMap.keySet()) {
            ttlMap.computeIfPresent(key, (k, v) -> {
                if (v + config.gracePeriod() > now) {
                    return v;
                }
                return null;
            });
        }
    }

    @Override
    public synchronized void reportRead(T element, long TTL, TimeUnit unit) {
        ttlMap.putRemaining(element, TTL, unit);
    }

    @Override
    public synchronized Long reportWrite(T element, TimeUnit unit) {
        // Only add if there is a potentially cached read
        Long expiration = ttlMap.get(element);
        if (expiration == null || expiration < ttlMap.now()) {
            return null;
        }

        add(element);
        queue.addExpiration(element, expiration, TimeUnit.MILLISECONDS);
        return unit.convert(expiration - ttlMap.now(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isKnown(T element) {
        if (!ttlMap.containsKey(element)) {
            return false;
        }

        long ttl = ttlMap.get(element) - ttlMap.now() + config.gracePeriod();
        return ttl > 0;
    }
    
    @Override
    public List<Boolean> isKnown(List<T> elements){
        return elements.stream().map(this::isKnown).collect(Collectors.toList());
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
    public void softClear() {
        super.clear();
        queue.clear();
    }

    @Override
    public void migrateFrom(BloomFilter<T> source) {
        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        // Migrate CBF and FBF
        super.migrateFrom(source);

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
