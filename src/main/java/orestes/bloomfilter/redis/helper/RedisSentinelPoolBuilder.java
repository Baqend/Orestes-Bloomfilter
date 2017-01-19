package orestes.bloomfilter.redis.helper;

import redis.clients.jedis.JedisSentinelPool;

import java.util.Set;

public class RedisSentinelPoolBuilder extends RedisBasePoolBuilder<RedisSentinelPoolBuilder> {
    private String master;
    private Set<String> sentinels;

    public RedisSentinelPoolBuilder() {
    }

    public RedisSentinelPoolBuilder master(String master) {
        this.master = master;
        return this;
    }

    public RedisSentinelPoolBuilder sentinels(Set<String> sentinels) {
        this.sentinels = sentinels;
        return this;
    }

    public RedisPool build() {
        JedisSentinelPool pool = new JedisSentinelPool(master, sentinels, getPoolConfig(redisConnections), timeout, password, database);
        return new RedisPool(pool, null, host, port);
    }
}
