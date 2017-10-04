package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 04.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class ExpirationQueueRedis implements ExpirationQueue<String> {
    private static final int HASH_LENGTH = 8;
    private static final String PATTERN_SUFFIX = String.join("", Collections.nCopies(HASH_LENGTH, "?"));

    private final RedisPool pool;
    private final String key;

    /**
     * Creates an ExpirationQueueRedis.
     *
     * @param pool A Redis pool instance to use
     * @param key  The key used for the queue within Redis
     */
    public ExpirationQueueRedis(RedisPool pool, String key) {
        this.pool = pool;
        this.key = key;
        clear();
    }

    @Override
    public int size() {
        return pool.safelyReturn(p -> p.zcard(key).intValue());
    }

    @Override
    public boolean add(ExpiringItem<String> item) {
        pool.safelyDo(p -> {
            String hash;
            do {
                hash = createRandomHash();
            } while (p.zadd(key, item.getExpiration(), item.getItem() + hash) == 0);
        });
        return true;
    }

    private String createRandomHash() {
        return UUID.randomUUID().toString().substring(0, HASH_LENGTH);
    }

    @Override
    public Collection<ExpiringItem<String>> getNonExpired() {
        List<Tuple> result = new ArrayList<>();
        pool.safelyDo(p -> {
            String cursor = "0";
            do {
                final ScanResult<Tuple> scanResult = p.zscan(key, cursor);
                result.addAll(scanResult.getResult());
                cursor = scanResult.getStringCursor();
            } while (!cursor.equals("0"));
        });

        return result.stream()
            .map(tuple -> new ExpiringItem<>(tuple.getElement(), (long) tuple.getScore()))
            .collect(Collectors.toList());
    }

    @Override
    public void clear() {
        pool.safelyDo(p -> p.del(key));
    }

    @Override
    public boolean contains(String item) {
        return pool.safelyReturn(
            p -> !p.zscan(key, "0", new ScanParams().match(item + PATTERN_SUFFIX)).getResult().isEmpty());
    }

    @Override
    public boolean remove(String item) {
        throw new UnsupportedOperationException("Cannot remove from ExpirationQueueRedis");
    }
}
