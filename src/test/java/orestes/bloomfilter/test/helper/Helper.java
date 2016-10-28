package orestes.bloomfilter.test.helper;


import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import orestes.bloomfilter.redis.helper.RedisSentinelConfiguration;
import orestes.bloomfilter.redis.helper.RedisStandaloneConfiguration;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Set;

public class Helper {
    public static String host = "localhost";
    public static int port = 6379;
    static int connections = 10;

    // There is a bug in Jedis 2.9.0 where 'localhost' isn't resolving to 127.0.0.1 like the binding shows so force
    // to your machine's IP. Note you may have to change the redis.conf and sentinel.conf files to use the IP too.
    static String sentinelHostName="10.77.175.110";
    static int sentinelRedisPort = 6380;

    public static Jedis getJedis() {
        return new Jedis(host, port);
    }

    public static Jedis getSentinelJedis() { return new Jedis(sentinelHostName, sentinelRedisPort);}

    public static RedisPool getPool() {
        return new RedisPool(host, port, connections, null, false);
    }

    public static <T> BloomFilterMemory<T> createFilter(int m, int k, HashMethod hm) {
        return new BloomFilterMemory<>(new FilterBuilder(m, k).hashFunction(hm).complete());
    }

    public static <T> BloomFilterMemory<T> createFilter(int n, double p, HashMethod hm) {
        return new BloomFilterMemory<>(new FilterBuilder(n, p).hashFunction(hm).complete());
    }

    public static <T> CountingBloomFilter<T> createCountingFilter(int m, int k, HashMethod hm) {
        return new FilterBuilder(m, k).hashFunction(hm).buildCountingBloomFilter();
    }

    public static <T> CountingBloomFilterMemory<T> createCountingFilter(int n, double p, HashMethod hm) {
        return new CountingBloomFilterMemory<>(new FilterBuilder(n, p).hashFunction(hm).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisFilter(String name, int m, int k, HashMethod hm, boolean overwrite) {
        return new BloomFilterRedis<>(new FilterBuilder(m, k).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisFilter(String name, int n, double p, HashMethod hm, boolean overwrite) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisPoolFilter(String name, int n, double p, HashMethod hm, boolean overwrite) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .pool(new RedisPool(new RedisStandaloneConfiguration(host, port), connections))
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisSentinelFilter(String name, int n, double p, HashMethod hm, boolean overwrite) {
        Set<String> sentinels = new HashSet<>();
        sentinels.add(sentinelHostName + ":16380");
        sentinels.add(sentinelHostName + ":16381");
        sentinels.add(sentinelHostName + ":16382");

        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .pool(new RedisPool(new RedisSentinelConfiguration("redis-cluster", sentinels, 100), connections))
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> CountingBloomFilterRedis<T> createCountingRedisFilter(String name, int m, int k, HashMethod hm, boolean overwrite) {
        return new CountingBloomFilterRedis<>(new FilterBuilder(m, k).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> CountingBloomFilterRedis<T> createCountingRedisFilter(String name, int n, double p, HashMethod hm, boolean overwrite) {
        return new CountingBloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static void cleanupRedis() {
        getJedis().flushAll();
    }

    public static void cleanupRedisSentinel() {
        getSentinelJedis().flushAll();
    }
}
