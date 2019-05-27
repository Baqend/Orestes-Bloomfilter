package orestes.bloomfilter.redis.helper;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the Redis keys for the Redis Bloom Filters
 */
public class RedisKeys {

    //Redis key constants
    public static final String N_KEY = "n";
    public static final String M_KEY = "m";
    public static final String K_KEY = "k";
    public static final String C_KEY = "c";
    public static final String P_KEY = "p";
    public static final String HASH_METHOD_KEY = "hashmethod";
    public final String BITS_KEY;
    public final String COUNTS_KEY;
    public final String TTL_KEY;
    public final String EXPIRATION_QUEUE_KEY;

    public RedisKeys(String instanceName) {
        this.BITS_KEY = instanceName + ":bits";
        this.COUNTS_KEY = instanceName + ":counts";
        this.TTL_KEY = instanceName + ":ttl";
        this.EXPIRATION_QUEUE_KEY = instanceName + ":queue";
    }


    public FilterBuilder persistConfig(RedisPool pool, FilterBuilder builder) {
        return pool.safelyReturn(jedis -> {
            FilterBuilder newConfig = null;
            //Retry on concurrent changes
            while (newConfig == null) {
                if (!builder.overwriteIfExists() && jedis.exists(builder.name())) {
                    newConfig = this.applyRedisConfigMap(jedis.hgetAll(builder.name()), builder, pool);
                    newConfig.complete();
                } else {
                    builder.complete();
                    Map<String, String> hash = this.buildRedisConfigMap(builder);
                    jedis.watch(builder.name());
                    Transaction t = jedis.multi();
                    hash.forEach((k, v) -> t.hset(builder.name(), k, v));
                    if (t.exec() != null) {
                        newConfig = builder;
                    }
                }
            }
            return newConfig;
        });
    }

    private Map<String, String> buildRedisConfigMap(FilterBuilder config) {
        Map<String, String> map = new HashMap<>();
        map.put(P_KEY, String.valueOf(config.falsePositiveProbability()));
        map.put(M_KEY, String.valueOf(config.size()));
        map.put(K_KEY, String.valueOf(config.hashes()));
        map.put(N_KEY, String.valueOf(config.expectedElements()));
        map.put(C_KEY, String.valueOf(config.countingBits()));
        map.put(HASH_METHOD_KEY, config.hashMethod().name());
        return map;
    }

    private FilterBuilder applyRedisConfigMap(Map<String, String> map, FilterBuilder config, RedisPool pool) {
        config.pool(pool);
        config.falsePositiveProbability(Double.valueOf(map.get(P_KEY)));
        config.size(Integer.valueOf(map.get(M_KEY)));
        config.hashes(Integer.valueOf(map.get(K_KEY)));
        config.expectedElements(Integer.valueOf(map.get(N_KEY)));
        config.countingBits(Integer.valueOf(map.get(C_KEY)));
        config.hashFunction(HashMethod.valueOf(map.get(HASH_METHOD_KEY)));
        return config;
    }

}
