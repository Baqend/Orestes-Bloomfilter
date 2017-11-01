package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExpiringBloomFilterRedis<T> extends CountingBloomFilterRedis<T> implements ExpiringBloomFilter<T> {
    private static final String REPORT_READ_LUA_SCRIPT = "local current = redis.call('get', KEYS[1]); " + "if current == false or tonumber(ARGV[1]) > tonumber(current) then " + "  redis.call('psetex', KEYS[1], ARGV[2], ARGV[1]) " + "end";

    private final Clock clock;
    private ExpirationQueue<T> queue;
    private final String reportReadLuaScriptHandle;

    public ExpiringBloomFilterRedis(FilterBuilder builder) {
        this(builder, true);
    }

    public ExpiringBloomFilterRedis(FilterBuilder builder, boolean initQueue) {
        super(builder);

        this.clock = pool.getClock();

        if (initQueue) {
            // Init expiration queue which removes elements from Bloom filter if entry expires
            this.queue = new ExpirationQueueMemory<>(this::onExpire);
        }

        // Load the "report read" Lua script
        this.reportReadLuaScriptHandle = pool.safelyReturn(jedis -> jedis.scriptLoad(REPORT_READ_LUA_SCRIPT));
    }

    /**
     * Sets the given expiration queue.
     *
     * @param queue The expiration queue to set
     */
    public void setQueue(ExpirationQueue<T> queue) {
        if (this.queue != null) {
            throw new RuntimeException("You cannot set a queue if there is already one.");
        }
        this.queue = queue;
    }

    /**
     * Handler for the expiration queue, removes entry from Bloom filter
     *
     * @param entry The entry which expired in the ExpirationQueue
     */
    private void onExpire(ExpiringItem<T> entry) {
        this.removeAndEstimateCount(entry.getItem());
    }

    /**
     * @return current timestamp in milliseconds
     */
    private Long now() {
        return clock.instant().toEpochMilli();
    }

    /**
     * @param TTL  the TTL to convert
     * @param unit the unit of the TTL
     * @return timestamp from TTL in milliseconds
     */
    private Long ttlToTimestamp(long TTL, TimeUnit unit) {
        return clock.instant().plusMillis(TimeUnit.MILLISECONDS.convert(TTL, unit)).toEpochMilli();
    }

    @Override
    public boolean isCached(T element) {
        Long remaining = getRemainingTTL(element, TimeUnit.MILLISECONDS);
        return remaining != null && remaining > 0;
    }

    @Override
    public Long getRemainingTTL(T element, TimeUnit unit) {
        return pool.safelyReturn(jedis -> {
            String tsString = jedis.get(key(element));
            return tsString != null ? unit.convert(Long.valueOf(tsString) - now(), TimeUnit.MILLISECONDS) : null;
        });
    }

    @Override
    public List<Long> getRemainingTTLs(List<T> elements, TimeUnit unit) {
        //Mget limitation: will be stored in Redis memory before being send, i.e. only scales to hundreds of thousands elements
        return pool.safelyReturn(jedis -> {
            String[] keys = elements.stream().map(this::key).toArray(String[]::new);
            List<String> vals = jedis.mget(keys);
            return vals.stream()
                .map(
                    tsString -> tsString != null ? unit.convert(Long.valueOf(tsString) - now(), TimeUnit.MILLISECONDS) :
                        null)
                .collect(Collectors.toList());
        });
    }

    @Override
    public void reportRead(T element, long TTL, TimeUnit unit) {
        // Create timestamp from TTL
        String ts = ttlToTimestamp(TTL, unit).toString();
        // Convert to TTL string Redis format
        String ttl_str = String.valueOf(TimeUnit.MILLISECONDS.convert(TTL, unit));
        pool.safelyDo(jedis -> jedis.evalsha(reportReadLuaScriptHandle, Collections.singletonList(key(element)),
            Arrays.asList(ts, ttl_str)));
    }

    @Override
    public Long reportWrite(T element, TimeUnit unit) {
        Long remaining = getRemainingTTL(element, TimeUnit.NANOSECONDS);
        if (remaining != null && remaining >= 0) {
            add(element);
            queue.addTTL(element, remaining);
        }
        return remaining != null ? unit.convert(remaining, TimeUnit.NANOSECONDS) : null;
    }

    @Override
    public List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        List<Long> remainingTTLs = getRemainingTTLs(elements, TimeUnit.NANOSECONDS);
        List<T> filteredElements = new LinkedList<>();
        List<Long> reportedTTLs = new LinkedList<>();
        for (int i = 0; i < remainingTTLs.size(); i++) {
            Long remaining = remainingTTLs.get(i);
            if (remaining != null && remaining >= 0) {
                reportedTTLs.add(unit.convert(remaining, TimeUnit.NANOSECONDS));

                T element = elements.get(i);
                filteredElements.add(element);
                queue.addTTL(element, remaining);
            } else {
                reportedTTLs.add(null);
            }
        }
        addAll(filteredElements);
        return reportedTTLs;
    }

    @Override
    public void clear() {
        // Clear CBF
        super.clear();
        // During init, ONLY clear CBF
        if (queue == null) {
            return;
        }
        // Clear Queue
        queue.clear();
        // Clear TTLs
        pool.safelyDo(this::clearTTLs);
    }

    public String key(T element) {
        return keys.TTL_KEY + element.toString();
    }


    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return toMemoryFilter();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpirations() {
        // TODO Refactor TTL list to use a redis map before implementing this.
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<ExpiringItem<T>> streamExpiringBFItems() {
        return queue.streamEntries();
    }

    @Override
    public CountingBloomFilterRedis<T> migrateFrom(BloomFilter<T> source) {
        // Migrate CBF and FBF
        super.migrateFrom(source);

        if (!(source instanceof ExpiringBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        // TODO migrate TLL list
        ((ExpiringBloomFilter<T>) source).streamExpirations();

        // migrate queue
        ((ExpiringBloomFilter<T>) source).streamExpiringBFItems().forEach(queue::add);

        return this;
    }

    /**
     * Clears TTLs from Redis.
     *
     * @param jedis The Jedis instance to use.
     */
    protected void clearTTLs(Jedis jedis) {
        final ScanParams params = new ScanParams().match(keys.TTL_KEY + "*").count(500);
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, params);
            cursor = scanResult.getStringCursor();

            List<String> result = scanResult.getResult();
            if (!result.isEmpty()) {
                jedis.del(result.toArray(new String[result.size()]));
            }
        } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
    }
}
