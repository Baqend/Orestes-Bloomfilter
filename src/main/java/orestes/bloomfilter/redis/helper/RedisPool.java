package orestes.bloomfilter.redis.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.Pool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Encapsulates a Connection Pool and offers convenience methods for safe access through Java 8 Lambdas.
 */
public class RedisPool {
    private static final Logger LOG = LoggerFactory.getLogger(RedisPool.class);

    private final Pool<Jedis> pool;
    private  String host;
    private  int port;
    private  int redisConnections;

    private  boolean ssl = false;
    private List<RedisPool> slavePools;
    private Random random;

    public RedisPool(String host, int port, int redisConnections, String password, boolean ssl) {
        this.host = host;
        this.port = port;
        this.redisConnections = redisConnections;
        this.ssl = ssl;
        this.pool = createJedisPool(host, port, redisConnections, password, ssl);
    }

    public RedisPool(String host, int port, int redisConnections, boolean ssl) {
        this(host, port, redisConnections, null, false);
    }

    public RedisPool(String host, int port, int redisConnections, Set<Entry<String, Integer>> readSlaves, String password, boolean ssl) {
        this(host, port, redisConnections, password, ssl);
        setupReadSlaves(readSlaves);
    }

    public RedisPool(RedisSentinelConfiguration sentinelConfiguration, int redisConnections) {
        this.redisConnections = redisConnections;
        this.pool = createSentinelPool(sentinelConfiguration, redisConnections);
    }

    public RedisPool(RedisStandaloneConfiguration standaloneConfiguration, int redisConnections) {
        this.redisConnections = redisConnections;
        this.pool = createJedisPool(standaloneConfiguration.getHost(), standaloneConfiguration.getPort(), redisConnections,
                standaloneConfiguration.getPassword(), standaloneConfiguration.isSsl());
        setupReadSlaves(standaloneConfiguration.getReadSlaves());
    }

    public RedisPool(Pool<Jedis> externallyManaged) {
        this.pool = externallyManaged;
    }

    public Pool<Jedis> getInternalPool() {
        return pool;
    }

    private void setupReadSlaves(Set<Entry<String, Integer>> readSlaves) {
        if (readSlaves != null && !readSlaves.isEmpty()) {
            slavePools = new ArrayList<>();
            random = new Random();
            for (Entry<String, Integer> slave : readSlaves) {
                slavePools.add(new RedisPool(slave.getKey(), slave.getValue(), redisConnections, ssl));
            }
        }
    }

    private JedisPool createJedisPool(String host, int port, int redisConnections, String password, boolean ssl) {

        if (password == null || password.length() == 0) {
            return new JedisPool(getPoolConfig(redisConnections), host, port, Protocol.DEFAULT_TIMEOUT * 4, ssl);
        } else {
            return new JedisPool(getPoolConfig(redisConnections), host, port, Protocol.DEFAULT_TIMEOUT * 4, password, ssl);
        }
    }

    private JedisSentinelPool createSentinelPool(RedisSentinelConfiguration sentinelConfiguration, int redisConnections) {
        if(sentinelConfiguration.getPassword() == null) {
            return new JedisSentinelPool(sentinelConfiguration.getMasterName(),
                    sentinelConfiguration.getSentinels(), getPoolConfig(redisConnections), sentinelConfiguration.getTimeout());
        }
        else {
            return new JedisSentinelPool(sentinelConfiguration.getMasterName(),
                    sentinelConfiguration.getSentinels(), getPoolConfig(redisConnections), sentinelConfiguration.getTimeout(), sentinelConfiguration.getPassword());
        }

    }

    private JedisPoolConfig getPoolConfig(int redisConnections) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setBlockWhenExhausted(true);
        config.setMaxTotal(redisConnections);
        return config;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getRedisConnections() {
        return redisConnections;
    }

    public boolean getSsl() {
        return ssl;
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
                try {
                    //pubsub has its own Redis connection
                    Jedis jedis = new Jedis(redisHost, redisPort);
                    jedis.ping();
                    connected = true;
                    whenConnected.accept(jedis);
                } catch (JedisConnectionException | JedisDataException e) {
                    connected = false;
                    if (abort.apply(e)) {
                        break;
                    } else {
                        LOG.error("Could not establish connection to Redis server.", e);
                        //Rate Limit to 4 reconnects per second
                        try {
                            Thread.sleep(250);
                        } catch (InterruptedException e1) {
                            LOG.error("Interrupted {}", e1);
                        }
                    }
                }
            }
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
