package orestes.bloomfilter.redis.helper;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Encapsulates a Connection Pool and offers convenience methods for safe access through Java 8 Lambdas.
 */
public class RedisPool {

    private final JedisPool pool;
    private List<RedisPool> slavePools;
    private Random random;

    public RedisPool(String host, int port, int redisConnections) {
        this.pool = createJedisPool(host, port, redisConnections);
    }

    public RedisPool(String host, int port, int redisConnections, Set<Entry<String,Integer>> readSlaves) {
        this(host, port, redisConnections);
        if(readSlaves!=null && !readSlaves.isEmpty()) {
            slavePools = new ArrayList<>();
            random = new Random();
            for (Entry<String, Integer> slave : readSlaves) {
                slavePools.add(new RedisPool(slave.getKey(), slave.getValue(), redisConnections));
            }
        }
    }

    private JedisPool createJedisPool(String host, int port, int redisConnections) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setBlockWhenExhausted(true);
        config.setMaxTotal(redisConnections);
        return new JedisPool(config, host, port);
    }

    public RedisPool allowingSlaves() {
        if(slavePools == null)
            return this;
        int index = random.nextInt(slavePools.size());
        return slavePools.get(index);
    }

    public void safelyDo(Consumer<Jedis> f) {
        safelyReturn(jedis -> {
            f.accept(jedis);
            return null;
        });
    }

    public <T> T safelyReturn(Function<Jedis, T> f) {
        try (Jedis jedis = pool.getResource()) {
            return f.apply(jedis);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> transactionallyDo(Consumer<Pipeline> f, String... watch) {
        return (List<T>) safelyReturn(jedis -> {
            Pipeline p = jedis.pipelined();
            if(watch.length != 0)
                p.watch(watch);
            p.multi();
            f.accept(p);
            Response<List<Object>> exec = p.exec();
            p.sync();
            return exec.get();
        });
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> transactionallyRetry(Consumer<Pipeline> f, String... watch) {
        while(true) {
            List<T> result = transactionallyDo(f, watch);
            if(result != null)
                return result;
        }
    }

    public void destroy() {
        pool.destroy();
    }
}
