package orestes.bloomfilter.redis.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.Pool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Encapsulates a Connection Pool and offers convenience methods for safe access through Java 8 Lambdas.
 */
public class RedisPool {
    private static final Logger LOG = LoggerFactory.getLogger(RedisPool.class);

    private final Pool<Jedis> pool;
    private final List<RedisPool> slavePools;
    private final Random random;
    private final String host;
    private final int port;

    /**
     * Creates a builder for a standalone RedisPool
     * @return A builder for a standalone redisPool
     */
    public static final RedisStandalonePoolBuilder builder() {
        return new RedisStandalonePoolBuilder();
    }

    /**
     * Creates a builder for a sentinel RedisPool
     * @return A builder for a sentinel redisPool
     */
    public static final RedisSentinelPoolBuilder sentinelBuilder() {
        return new RedisSentinelPoolBuilder();
    }

    protected RedisPool(Pool<Jedis> pool, List<RedisPool> slavePools, String host, int port) {
        this.pool = pool;
        this.host = host;
        this.port = port;

        if (slavePools != null && !slavePools.isEmpty()) {
            this.slavePools = slavePools;
            this.random = new Random();
        } else {
            this.slavePools = slavePools;
            this.random = null;
        }
    }

    public Pool<Jedis> getInternalPool() {
        return pool;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RedisPool allowingSlaves() {
        if (slavePools == null) {
            return this;
        }
        int index = random.nextInt(slavePools.size());
        return slavePools.get(index);
    }

    public Jedis getResource() {
        return pool.getResource();
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

    public <T> void safeForEach(Collection<T> collection, BiConsumer<Pipeline, T> f) {
        safelyReturn(jedis -> {
            Pipeline p = jedis.pipelined();
            collection.stream().forEach(e -> f.accept(p, e));
            p.sync();
            return null;
        });
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> transactionallyDo(Consumer<Pipeline> f, String... watch) {
        return (List<T>) safelyReturn(jedis -> {
            Pipeline p = jedis.pipelined();
            if (watch.length != 0) {
                p.watch(watch);
            }
            p.multi();
            f.accept(p);
            Response<List<Object>> exec = p.exec();
            p.sync();
            return exec.get();
        });
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> transactionallyRetry(Consumer<Pipeline> f, String... watch) {
        while (true) {
            List<T> result = transactionallyDo(f, watch);
            if (result != null) {
                return result;
            }
        }
    }


    public Clock getClock() {
        List<String> time = this.safelyReturn(Jedis::time);
        Instant local = Instant.now();
        //Format: [0]: unix ts [1]: microseconds
        Instant redis = Instant.ofEpochSecond(Long.valueOf(time.get(0)), Long.valueOf(time.get(1)) * 1000);
        return Clock.offset(Clock.systemDefaultZone(), Duration.between(local, redis));
    }


    /**
     * Start an automatically reconnecting Thread with a dedicated connection to Redis (e.g. for PubSub or blocking
     * pops)
     *
     * @param redisHost     host
     * @param redisPort     port
     * @param whenConnected executed when the connections is active
     * @param abort         lambda that allows to abort processing when an error occurs
     * @return the started thread
     */
    public static Thread startThread(String redisHost, int redisPort, Consumer<Jedis> whenConnected, Function<Exception, Boolean> abort) {
        Thread thread = new Thread(() -> {
            boolean connected = false;
            while (!connected && !Thread.currentThread().isInterrupted()) {
                try (Jedis jedis = new Jedis(redisHost, redisPort)) {
                    //pubsub has its own Redis connection
                    jedis.ping();
                    connected = true;
                    LOG.info("PubSub Redis connection established.");
                    whenConnected.accept(jedis);
                } catch (Exception e) {
                    connected = false;
                    if (abort.apply(e)) {
                        LOG.info("PubSub Redis connection aborted.", e);
                        break;
                    } else {
                        LOG.warn("PubSub Redis connection failed with an exception:", e);
                        //Rate Limit to 4 reconnects per second
                        try {
                            Thread.sleep(250);
                        } catch (InterruptedException e1) {
                            LOG.warn("PubSub Redis connection Interrupted", e1);
                        }
                    }
                }
            }
            LOG.info("PubSub Redis connection closed.");
        });
        thread.start();
        return thread;
    }

    public Thread startThread(Consumer<Jedis> whenConnected) {
        return startThread(whenConnected, (ex) -> false);
    }

    public Thread startThread(Consumer<Jedis> whenConnected, Function<Exception, Boolean> abort) {
        return startThread(host, port, whenConnected, abort);
    }

    public void destroy() {
        pool.destroy();
    }
}
