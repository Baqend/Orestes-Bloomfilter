package orestes.bloomfilter.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import orestes.bloomfilter.BloomFilter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * A persistent bloom filter backed by the Redis key value store. Internally it operates on the <i>setbit</i> and
 * <i>getbit</i> operations of Redis. If you need to remove elements from the bloom filter, please use a counting bloom
 * filter, e.g. {@link CBloomFilterRedis}. The performance of this data structure is very good, as operations are grouped
 * into fast transactions, minimizing the network overhead of all bloom filter operations to one round trip to Redis.
 *
 * @param <T>
 */
public class BloomFilterRedis<T> extends BloomFilter<T> {
    private String name;
    private boolean hasPopulationCount;
    private Jedis jedis;
    private Pipeline p;
    private int n;

    private final static String POPULATION_SUFFIX = ":population";
    private final static String CONFIG_SUFFIX = ":config";
    private final static String M_KEY = "m";
    private final static String K_KEY = "k";
    private final static String TYPE_KEY = "t";
    private final static String HASH_KEY = "h";
    private final static String N_KEY = "n";

    public void useConnection(Jedis a_jedis) {
        jedis = a_jedis;
        getBloom().useConnection(a_jedis);
    }


    public static <V> BloomFilterRedis<V> loadFilter(Jedis jedis, String name) {
        if (!jedis.exists(name)) return null;

        Pipeline p = jedis.pipelined();
        p.multi();
        p.hget(BloomFilterRedis.buildConfigKeyName(name), M_KEY);       //0
        p.hget(BloomFilterRedis.buildConfigKeyName(name), K_KEY);       //1
        p.hget(BloomFilterRedis.buildConfigKeyName(name), TYPE_KEY);    //2
        p.hget(BloomFilterRedis.buildConfigKeyName(name), HASH_KEY);    //3
        p.hget(BloomFilterRedis.buildConfigKeyName(name), N_KEY);    //4
        Response<List<Object>> result = p.exec();
        p.sync();
        List<Object> items = result.get();

        int m = Integer.parseInt((String) items.get(0));
        int k = Integer.parseInt((String) items.get(1));
        int n = Integer.parseInt((String)items.get(4));
        String hashType = (String) items.get(3);

        boolean populationType = "1".equalsIgnoreCase((String) items.get(2));

        int separatorOffset = hashType.indexOf(":");
        if (separatorOffset >= 0) {
            String cryptoMethod = hashType.substring(separatorOffset + 1);
            return new BloomFilterRedis<>(name, jedis, m, k, cryptoMethod, populationType, n);
        } else {
            return new BloomFilterRedis<>(name, jedis, m, k, HashMethod.valueOf(hashType), populationType, n);
        }

    }

    private void persistToRedis(Jedis connection) {
        String configKeyName = buildConfigKeyName(name);
        connection.watch(configKeyName);
        boolean exists =     connection.exists(configKeyName);
        if (!(exists && connection.hlen(configKeyName) > 2) ) {
            // interesting race condition, if the hash function was defined when the BF was created the
            // Redis hash may exist before the Redis Bitmap.
            //
            Transaction t = connection.multi();
            t.hset(configKeyName, M_KEY, Integer.toString(m));
            t.hset(configKeyName, K_KEY, Integer.toString(k));
            t.hset(configKeyName,TYPE_KEY , hasPopulationCount ? "1" : "0");
            t.hset(configKeyName, HASH_KEY, buildHashPersistenceValue());
            t.hset(configKeyName, N_KEY, Integer.toString(n));
            t.exec();
        }
    }

    public static boolean removeFilter(Jedis jedis, String name) {
        if (!jedis.exists(name)) return true;

        Pipeline p = jedis.pipelined();
        p.multi();
        p.del(name);
        p.del(BloomFilterRedis.buildConfigKeyName(name));
        p.del(BloomFilterRedis.buildPopulationKeyName(name));
        p.exec();
        p.sync();

        return true;

    }

    public static <V> BloomFilterRedis<V> createFilter(Jedis jedis, String name, double n, double p, HashMethod method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = (int)n;
        return new BloomFilterRedis<>(name, jedis, optimalM(n, p), optimalK(n, optimalM(n, p)), method, false, size);
    }

    public static <V> BloomFilterRedis<V> createPopulationFilter(Jedis jedis, String name, double n, double p, HashMethod method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = (int)n;
        return new BloomFilterRedis<>(name, jedis, optimalM(n, p), optimalK(n, optimalM(n, p)), method, true, size);
    }

    public static <V> BloomFilterRedis<V> createFilter(Jedis jedis, String name, int m, int k, HashMethod method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = 0;       // no way of knowing how many elements we sized it for
        return new BloomFilterRedis<>(name, jedis, m, k, method, false, size);
    }

    public static <V> BloomFilterRedis<V> createPopulationFilter(Jedis jedis, String name, int m, int k, HashMethod method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = 0;       // no way of knowing how many elements we sized it for
        return new BloomFilterRedis<>(name, jedis, m, k, method, true, size);
    }

    public static <V> BloomFilterRedis<V> createCryptographicFilter(Jedis jedis, String name, int m, int k, String method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = 0;       // no way of knowing how many elements we sized it for
        return new BloomFilterRedis<>(name, jedis, m, k, method, false, size);
    }

    public static <V> BloomFilterRedis<V> createCrypyographicPopulationFilter(Jedis jedis, String name, int m, int k, String method) {
        if (jedis.exists(name)) throw new IllegalArgumentException("Filter with this name exists.");
        int size = 0;       // no way of knowing how many elements we sized it for
        return new BloomFilterRedis<>(name, jedis, m, k, method, true, size);
    }

    /**
     * Creates a new persistent Bloomfilter backed by Redis.
     *
     * @param m The size of the bloom filter in bits.
     * @param k The number of hash functions to use.
     */
    protected BloomFilterRedis(String name, Jedis jedis, int m, int k, HashMethod method, boolean hasPopulation, int size) {
        super(new RedisBitSet(jedis, name, m), m, k);
        this.jedis = jedis;
        this.name = name;
        this.hasPopulationCount = hasPopulation;
        this.n = size;
        setHashMethod(method);

        persistToRedis(jedis);
    }

    protected BloomFilterRedis(String name, Jedis jedis, int m, int k, String method, boolean hasPopulation, int size) {
        super(new RedisBitSet(jedis, name, m), m, k);
        this.jedis = jedis;
        this.name = name;
        this.hasPopulationCount = hasPopulation;
        this.hashMethod = HashMethod.Cryptographic;
        this.setCryptographicHashFunction(method);
        this.n = size;

        persistToRedis(jedis);
    }

    private String buildHashPersistenceValue() {
        HashMethod method = getHashMethod();
        if (method == HashMethod.Cryptographic) {
            return method.toString() + ":" + getCryptographicHashFunctionName();
        }
        return method.toString();
    }

    public static String buildConfigKeyName(String name) {
        return name + CONFIG_SUFFIX;
    }

    public static String buildPopulationKeyName(String name) {
        return name + POPULATION_SUFFIX;
    }

    public int getN() {
        return n;
    }

    @Override
    public long getPopulation() {
        long pop = -1;
        if (hasPopulationCount) {
            String val = jedis.get(buildPopulationKeyName(name));
            if (val != null && val.length() != 0) {
                pop = Long.parseLong(val);
            } else {
                pop = 0;
            }
        }
        return pop;
    }

    @Override
    public void setHashMethod(HashMethod hashMethod) {
        super.setHashMethod(hashMethod);
        if (jedis != null) {
            jedis.hset(buildConfigKeyName(name),HASH_KEY,  getCryptographicHashFunctionName());
        }
    }

    @Override
    public void setCryptographicHashFunction(String hashFunctionName) {
        super.setCryptographicHashFunction(hashFunctionName);
        if (jedis != null) {
            jedis.hset(buildConfigKeyName(name),HASH_KEY,  getCryptographicHashFunctionName());
        }
    }

    private boolean internalAdd(byte[] value) {
        begin();
        for (int position : hash(value)) {
            setBit(position);
        }
        List<Object> results = commit();

        if (results == null)
            return internalAdd(value);
        else {
            boolean wasSet = false;
            for (Object bit : results) {
                if (!(Boolean) bit) wasSet = true;
            }
            return wasSet;
        }
    }

    @Override
    public boolean add(byte[] value) {
        boolean response = internalAdd(value);
        if (response && hasPopulationCount) {
            jedis.incr(buildPopulationKeyName(this.name));
        }
        return response;
    }

    private List<Boolean> internalAddAll(Collection<T> values) {
        List<Boolean> added = new ArrayList<>();
        begin();
        for (T val : values) {
            for (int position: hash(val.toString().getBytes(getDefaultCharset()))) {
                 setBit(position);
            }
        }
        List<Object> results = commit();
        if (results == null)  {
            added = internalAddAll(values);
        }
        else {
            int numHashes = k;
            boolean wasAdded = false;
            int numProcessed = 0;
            for (Object item : results) {
                if (!(Boolean) item) wasAdded = true;
                if ((numProcessed + 1) % numHashes == 0) {
                    added.add(wasAdded);
                    wasAdded = false;
                }
                numProcessed++;
            }
            return added;
        }

        return added;
    }

    @Override
    public List<Boolean> addAll(Collection<T> values) {
        List<Boolean> added = internalAddAll(values);
        if (hasPopulationCount) {
            int numAdded = 0;
            for (Boolean item : added) {
                if (item) numAdded++;
            }
            if (numAdded != 0) jedis.incrBy(buildPopulationKeyName(this.name), numAdded);
        }
        return added;
    }

    public List<Boolean> contains(Collection<T> values) {
        List<Boolean> results = new ArrayList<>();
        Pipeline pipeline = jedis.pipelined();
        pipeline.multi();
        for (T val: values) {
            for (int pos: hash(val.toString().getBytes(getDefaultCharset()))) {
                pipeline.getbit(name, pos);
            }
        }
        Response<List<Object>> result = pipeline.exec();
        pipeline.sync();
        List<Object> data = result.get();
        if (data == null) {
            results = contains(values);
        }   else {
            int numHashes = k;
            boolean isPresent = true;
            int numProcessed = 0;
            for (Object item : data) {
                if (!(Boolean) item) isPresent = false;
                if ((numProcessed + 1) % numHashes == 0) {
                    results.add(isPresent);
                    isPresent = true;
                }
                numProcessed++;
            }
        }
        return results;
    }

    @Override
    public boolean contains(byte[] value) {
        boolean result = getBloom().allSet(hash(value));
        return result;
    }

    @Override
    public void clear() {
        super.clear();
        if (hasPopulationCount) {
            jedis.set(buildPopulationKeyName(this.name), "0");
        }
    }


    private void begin() {
        p = jedis.pipelined();
        p.multi();
        getBloom().useContext(p);
    }

    private List<Object> commit() {
        Response<List<Object>> result = p.exec();
        p.sync();
        getBloom().leaveContext();
        return result.get();
    }

    private RedisBitSet getBloom() {
        return (RedisBitSet) bloom;
    }

    public void releaseConnection() {
        useConnection(null);
    }

}
