package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.cachesketch.ExpirationQueue.ExpiringItem;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExpiringBloomFilterRedis<T> extends CountingBloomFilterRedis<T> implements ExpiringBloomFilter<T> {
    private final Clock clock;
    private ExpirationQueue<T> queue;
    private final String read_lua = "local current = redis.call('get', KEYS[1]); if current == false or tonumber(ARGV[1]) > tonumber(current) then redis.call('psetex', KEYS[1], ARGV[2], ARGV[1]) end";
    private String read_lua_hash = "";


    public ExpiringBloomFilterRedis(FilterBuilder builder) {
        super(builder);
        this.clock = pool.getClock();
        this.queue = new ExpirationQueue<>(this::onExpire);
        this.read_lua_hash = pool.safelyReturn(jedis -> jedis.scriptLoad(read_lua));
    }


    private void onExpire(ExpiringItem<T> entry) {
        double current = this.removeAndEstimateCount(entry.getItem());
    }

    /**
     * @return current timestamp in second
     */
    private Long now() {
        return clock.instant().toEpochMilli();
    }

    /**
     * @param TTL
     * @param unit
     * @return timestamp from ttl in second
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
            return vals.stream().
                map(tsString -> tsString != null ? unit.convert(Long.valueOf(tsString) - now(), TimeUnit.MILLISECONDS) : null)
                .collect(Collectors.toList());
        });
    }

    @Override
    public void reportRead(T element, long TTL, TimeUnit unit) {
        String ttl_str = String.valueOf(TimeUnit.MILLISECONDS.convert(TTL, unit));
        String ts = ttlToTimestamp(TTL, unit).toString();
        pool.safelyDo(jedis -> {
            jedis.evalsha(read_lua_hash, Collections.singletonList(key(element)),
                Arrays.asList(ts, ttl_str));
        });
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
        for (int i = 0; i < remainingTTLs.size() ; i++) {
            Long remaining = remainingTTLs.get(i);
            T element = elements.get(i);
            if(remaining != null && remaining >= 0) {
                filteredElements.add(element);
                queue.addTTL(element, remaining);
            }
        }
        addAll(filteredElements);
        return reportedTTLs;
    }

    @Override
    public void clear() {
        //Clear CBF
        super.clear();
        //During init ONLY clear CBF
        if(queue == null) {
            return;
        }
        //Clear Queue
        queue.clear();
        //Clear TTLs
        pool.safelyDo(jedis -> {
            ScanParams params = new ScanParams().match(keys.TTL_KEY + "*").count(500);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, params);
                cursor = scanResult.getStringCursor();

                List<String> result = scanResult.getResult();
                if (!result.isEmpty()) {
                    jedis.del(result.toArray(new String[result.size()]));
                }
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        });
    }

    public String key(T element) {
        return keys.TTL_KEY + element.toString();
    }


    @Override
    public BloomFilter<T> getClonedBloomFilter() {
        return toMemoryFilter();
    }
}
