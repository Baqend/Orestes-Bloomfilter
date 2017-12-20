package orestes.bloomfilter.test.helper;


import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import org.junit.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

import java.util.*;

public class Helper {
    // Jedis automatically translates any address which is known to refer to a local IP to the system's hostname.
    // All Redis servers must listen on all networks (bind 0.0.0.0 in redis conf) to workaround this.
    public static String host = "127.0.0.1";
    public static int port = 6379;
    public static int slavePort = 6380;
    private static int connections = 10;
    
    private static final String sentinelHostName = host;
    private static final String sentinelClusterName = "bf-cluster";

    private static Set<String> getSentinelNodes() {
        Set<String> sentinels = new HashSet<>();
        sentinels.add(sentinelHostName + ":16385");
        sentinels.add(sentinelHostName + ":16386");
        sentinels.add(sentinelHostName + ":16387");
        return sentinels;
    }

    public static Jedis getJedis() {
        return new Jedis(host, port);
    }

    public static JedisSentinelPool getSentinelJedis() {
        return new JedisSentinelPool(sentinelClusterName, getSentinelNodes());}

    public static RedisPool getPool() {
        return RedisPool.builder()
            .host(host)
            .port(port)
            .redisConnections(connections)
            .build();
    }

    public static <T> BloomFilterMemory<T> createFilter(int m, int k, HashMethod hm) {
        return new BloomFilterMemory<>(new FilterBuilder(m, k).hashFunction(hm).complete());
    }

    public static <T> BloomFilterMemory<T> createFilter(int n, double p, HashMethod hm) {
        return new BloomFilterMemory<>(configure(n, p, hm));
    }

    public static FilterBuilder configure(int n, double p, HashMethod hm) {
        return new FilterBuilder(n, p).hashFunction(hm).complete();
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
        return createRedisFilter(name, n, p, hm, overwrite, Protocol.DEFAULT_DATABASE);
    }

    public static <T> BloomFilterRedis<T> createRedisFilter(String name, int n, double p, HashMethod hm, boolean overwrite, int database) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .database(database)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisFilterWithReadSlave(String name, int n, double p, HashMethod hm, boolean overwrite, String slaveName, int slavePort) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .addReadSlave(slaveName, slavePort)
                .overwriteIfExists(overwrite)
                .redisConnections(connections).complete());
    }

    public static <T> BloomFilterRedis<T> createRedisPoolFilter(String name, int n, double p, HashMethod hm, boolean overwrite, int database) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .pool(RedisPool.builder().host(host).port(port).database(database).redisConnections(connections).build())
                .overwriteIfExists(overwrite)
                .complete());
    }


    public static <T> BloomFilterRedis<T> createRedisSentinelFilter(String name, int n, double p, HashMethod hm, boolean overwrite, int database) {
        return new BloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .pool(RedisPool.sentinelBuilder()
                        .master(sentinelClusterName)
                        .sentinels(getSentinelNodes())
                        .database(database)
                        .redisConnections(connections)
                        .build())
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
        return createCountingRedisFilter(name, n, p, hm, overwrite, Protocol.DEFAULT_DATABASE);
    }

    public static <T> CountingBloomFilterRedis<T> createCountingRedisFilter(String name, int n, double p, HashMethod hm, boolean overwrite, int database) {
        return new CountingBloomFilterRedis<>(new FilterBuilder(n, p).hashFunction(hm)
                .redisBacked(true)
                .name(name)
                .redisHost(host)
                .redisPort(port)
                .overwriteIfExists(overwrite)
                .database(database)
                .redisConnections(connections).complete());
    }

    public static void cleanupRedis() {
        getJedis().flushAll();
    }

    public static void cleanupRedisSentinel() {
        getSentinelJedis().getResource().flushAll();
    }

    public static void assertBitsSet(BitSet actual, int ...expectedIndexes) {
        Assert.assertEquals("Cardinality should match the number of expected indexes", expectedIndexes.length, actual.cardinality());
        Assert.assertEquals("Length should match the max expected index plus one", max(expectedIndexes) + 1, actual.length());
        for (int index : expectedIndexes) {
            Assert.assertTrue(actual.get(index));
        }
    }

    public static void assertAllEqual(long expected, Map<Integer, Long> actual, int ...expectedIndexes) {
        Assert.assertEquals("Size should match the number of expected indexes", expectedIndexes.length, actual.size());
        for (int index : expectedIndexes) {
            Assert.assertTrue(actual.containsKey(index));
            Assert.assertEquals(expected, (long) actual.get(index));
        }
    }

    public static int max(int[] as) {
        int result = -1;
        for (int a : as) {
            result = Math.max(result, a);
        }
        return result;
    }
}
