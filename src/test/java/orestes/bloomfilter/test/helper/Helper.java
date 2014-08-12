package orestes.bloomfilter.test.helper;


import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Jedis;

public class Helper {
    public static String host = "localhost";
    public static int port = 6379;
    public static int connections = 10;

    public static Jedis getJedis() {
        return new Jedis(host, port);
    }

    public static RedisPool getPool() {
        return new RedisPool(host, port, connections, null);
    }

    public static <T> BloomFilterMemory<T> createFilter(int m, int k, HashMethod hm) {
        return new BloomFilterMemory<>(new FilterBuilder(m, k).hashFunction(hm).complete());
    }

    public static <T> BloomFilterMemory<T> createFilter(int n, double p, HashMethod hm) {
        return new BloomFilterMemory<>(new FilterBuilder(n, p).hashFunction(hm).complete());
    }

    public static <T> CountingBloomFilterMemory<T> createCountingFilter(int m, int k, HashMethod hm) {
        return new CountingBloomFilterMemory<>(new FilterBuilder(m, k).hashFunction(hm).complete());
    }

    public static <T> CountingBloomFilterMemory<T> createCountingFilter(int n, double p, HashMethod hm) {
        return new CountingBloomFilterMemory<>(new FilterBuilder(n, p).hashFunction(hm).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisFilter(String name, int m, int k, HashMethod hm) {
        return new BloomFilterRedis<>(new FilterBuilder(m, k).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(true)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisFilter(String name, int n, double p, HashMethod hm) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(true)
                .redisConnections(connections).complete());
    }

    public static <T> CountingBloomFilterRedis<T> createCountingRedisFilter(String name, int m, int k, HashMethod hm) {
        return new CountingBloomFilterRedis<>(new FilterBuilder(m, k).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(true)
                .redisConnections(connections).complete());
    }

    public static <T> CountingBloomFilterRedis<T> createCountingRedisFilter(String name, int n, double p, HashMethod hm) {
        return new CountingBloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(true)
                .redisConnections(connections).complete());
    }

    public static void cleanupRedis() {
        getJedis().flushAll();
    }
}
