package orestes.bloomfilter.redis.helper;

import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class RedisStandalonePoolBuilder extends RedisBasePoolBuilder<RedisStandalonePoolBuilder> {
    private Set<Map.Entry<String, Integer>> readSlaves = null;
    private boolean ssl = false;

    public RedisStandalonePoolBuilder() {}

    public RedisStandalonePoolBuilder readSlaves(Set<Map.Entry<String, Integer>> readSlaves) {
        this.readSlaves = readSlaves;
        return this;
    }

    public RedisStandalonePoolBuilder ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    public RedisPool build() {
        ArrayList<RedisPool> slavePools = null;
        if (readSlaves != null && !readSlaves.isEmpty()) {
            slavePools = new ArrayList<>();
            for (Map.Entry<String, Integer> slave : readSlaves) {
                String host = slave.getKey();
                Integer port = slave.getValue();
                slavePools.add(new RedisPool(createJedisPool(host, port), null, host, port));
            }
        }

        JedisPool pool = createJedisPool(host, port);

        return new RedisPool(pool, slavePools, host, port);
    }

    protected JedisPool createJedisPool(String host, int port) {
        return new JedisPool(getPoolConfig(redisConnections), host, port, timeout, password, database, ssl);
    }
}
