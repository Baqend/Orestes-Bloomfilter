package orestes.bloomfilter.redis.helper;

import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by fbuec on 19.01.2017.
 */
public class RedisBasePoolBuilder<B extends RedisBasePoolBuilder> {

    protected String host;
    protected int port;
    protected String password = null;
    protected int database;
    protected int redisConnections;
    protected int timeout;

    protected JedisPoolConfig getPoolConfig(int redisConnections) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setBlockWhenExhausted(true);
        config.setMaxTotal(redisConnections);
        return config;
    }

    public B host(String host) {
        this.host = host;
        return (B) this;
    }

    public B port(int port) {
        this.port = port;
        return (B) this;
    }

    public B timeout(int timeout) {
        this.timeout = timeout;
        return (B) this;
    }

    public B password(String password) {
        this.password = password;
        return (B) this;
    }

    public B database(int database) {
        this.database = database;
        return (B) this;
    }

    public B redisConnections(int redisConnections) {
        this.redisConnections = redisConnections;
        return (B) this;
    }
}
