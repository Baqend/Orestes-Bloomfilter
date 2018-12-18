package orestes.bloomfilter;

import orestes.bloomfilter.HashProvider.HashFunction;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.memory.BloomFilterMemory;
import orestes.bloomfilter.memory.CountingBloomFilter16;
import orestes.bloomfilter.memory.CountingBloomFilter32;
import orestes.bloomfilter.memory.CountingBloomFilter64;
import orestes.bloomfilter.memory.CountingBloomFilter8;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import orestes.bloomfilter.redis.helper.RedisPool;
import redis.clients.jedis.Protocol;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Builder for Bloom Filters.
 */
public class FilterBuilder implements Cloneable, Serializable {
    private boolean redisBacked = false;
    private boolean overwriteIfExists = false;
    private Integer expectedElements;
    private Integer size;
    private Integer hashes;
    private Integer countingBits = 16;
    private Double falsePositiveProbability;
    private String name = "";
    private String redisHost = "localhost";
    private Integer redisPort = 6379;
    private Integer redisConnections = 10;
    private boolean redisSsl = false;
    private HashMethod hashMethod = HashMethod.Murmur3KirschMitzenmacher;
    private HashFunction hashFunction = HashMethod.Murmur3KirschMitzenmacher.getHashFunction();
    private Set<Entry<String, Integer>> slaves = new HashSet<>();
    private static transient Charset defaultCharset = Charset.forName("UTF-8");
    private boolean done = false;
    private String password = null;
    private RedisPool pool;
    private int database = Protocol.DEFAULT_DATABASE;
    private long gracePeriod = TimeUnit.HOURS.toMillis(6);
    private long cleanupInterval = TimeUnit.HOURS.toMillis(1);

    /**
     * Constructs a new builder for Bloom filters and counting Bloom filters.
     */
    public FilterBuilder() {
    }

    /**
     * Constructs a new Bloom Filter Builder by specifying the expected size of the filter and the tolerable false
     * positive probability. The size of the BLoom filter in in bits and the optimal number of hash functions will be
     * inferred from this.
     *
     * @param expectedElements         expected elements in the filter
     * @param falsePositiveProbability tolerable false positive probability
     */
    public FilterBuilder(int expectedElements, double falsePositiveProbability) {
        this.expectedElements(expectedElements).falsePositiveProbability(falsePositiveProbability);
    }

    /**
     * Constructs a new Bloom Filter Builder using the specified size in bits and the specified number of hash
     * functions.
     *
     * @param size   bit size of the Bloom filter
     * @param hashes number of hash functions to use
     */
    public FilterBuilder(int size, int hashes) {
        this.size(size).hashes(hashes);
    }

    /**
     * Sets the number of expected elements. In combination with the tolerable false positive probability, this is used
     * to infer the optimal size and optimal number of hash functions of the filter.
     *
     * @param expectedElements number of expected elements.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder expectedElements(int expectedElements) {
        this.expectedElements = expectedElements;
        return this;
    }

    /**
     * Sets the size of the filter in bits.
     *
     * @param size size of the filter in bits
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Sets the tolerable false positive probability. In combination with the number of expected elements, this is used
     * to infer the optimal size and optimal number of hash functions of the filter.
     *
     * @param falsePositiveProbability the tolerable false
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder falsePositiveProbability(double falsePositiveProbability) {
        this.falsePositiveProbability = falsePositiveProbability;
        return this;
    }

    /**
     * Set the number of hash functions to be used.
     *
     * @param numberOfHashes number of hash functions used by the filter.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder hashes(int numberOfHashes) {
        this.hashes = numberOfHashes;
        return this;
    }

    /**
     * Sets the number of bits used for counting in case of a counting Bloom filter. For non-counting Bloom filters this
     * setting has no effect. <p><b>Default</b>: 16</p>
     *
     * @param countingBits Number of counting bits used by the counting Bloom filter
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder countingBits(int countingBits) {
        this.countingBits = countingBits;
        return this;
    }

    /**
     * Sets the name of the Bloom filter. If a redis-backed Bloom filter with the provided name exists and it is
     * compatible to this FilterBuilder configuration, it will be loaded and used. This behaviour can be changed by
     * {@link #overwriteIfExists(boolean)}. <p><b>Default</b>: ""</p>
     *
     * @param name The name of the filter
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets a password authentication for Redis.
     *
     * @param password The Redis PW
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder password(String password) {
        this.password = password;
        return this;
    }

    /**
     * Sets an existing RedisPool for reuse
     *
     * @param pool The RedisPool
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder pool(RedisPool pool) {
        this.redisBacked(true);
        this.pool = pool;
        return this;
    }

    /**
     * Instructs the FilterBuilder to build a Redis-Backed Bloom filters. <p><b>Default</b>: <tt>false</tt></p>
     *
     * @param redisBacked a boolean indicating whether redis should be used
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder redisBacked(boolean redisBacked) {
        this.redisBacked = redisBacked;
        return this;
    }

    /**
     * Sets the host of the backing Redis instance. <p><b>Default</b>: localhost</p>
     *
     * @param host the Redis host
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder redisHost(String host) {
        this.redisBacked = true;
        this.redisHost = host;
        return this;
    }

    /**
     * Sets the port of the backing Redis instance. <p><b>Default</b>: 6379</p>
     *
     * @param port the Redis port
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder redisPort(int port) {
        this.redisBacked = true;
        this.redisPort = port;
        return this;
    }


    /**
     * Sets the number of connections to use for Redis. <p><b>Default</b>: 10</p>
     *
     * @param numConnections the number of connections to use for Redis
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder redisConnections(int numConnections) {
        this.redisBacked = true;
        this.redisConnections = numConnections;
        return this;
    }

    /**
     * Enables or disables SSL connection to Redis. <p><b>Default</b>: false</p>
     *
     * @param ssl enables or disables SSL connection to Redis
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder redisSsl(boolean ssl) {
        this.redisBacked = true;
        this.redisSsl = ssl;
        return this;
    }

    /**
     * Sets whether any existing Bloom filter with same name should be overwritten in Redis. <p><b>Default</b>:
     * <tt>false</tt></p>
     *
     * @param overwrite boolean indicating whether to overwrite any existing filter with the same name
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder overwriteIfExists(boolean overwrite) {
        this.overwriteIfExists = overwrite;
        return this;
    }

    /**
     * Adds a read slave to speed up reading access (e.g. contains or getEstimatedCount) to normal and counting
     * Redis-backed Bloom filters. The read slave has to be a slave of the main Redis instance (this can be done in the
     * redis-cli using the SLAVEOF command). This setting might cause stale reads since Redis replication is
     * asynchronous. However anecdotally, in our experiments, we were unable to read any stale data - the replication
     * lag between both Redis instances was small than one round-trip time to Redis.
     *
     * @param host host of the Redis read slave
     * @param port port of the Redis read slave
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder addReadSlave(String host, int port) {
        slaves.add(new SimpleEntry<>(host, port));
        return this;
    }

    /**
     * Sets the method used to generate hash values. Possible hash methods are documented in the corresponding enum
     * {@link HashProvider.HashMethod}. <p><b>Default</b>: MD5</p>
     * <p>
     * For the generation of hash values the String representation of objects is used.
     *
     * @param hashMethod the method used to generate hash values
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder hashFunction(HashMethod hashMethod) {
        this.hashMethod = hashMethod;
        this.hashFunction = hashMethod.getHashFunction();
        return this;
    }

    /**
     * Uses a given custom hash function.
     *
     * @param hf the custom hash function
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder hashFunction(HashFunction hf) {
        this.hashFunction = hf;
        return this;
    }

    /**
     * Use a given database number.
     *
     * @param database number
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder database(int database) {
        this.database = database;
        return this;
    }

    public int database() {
        return database;
    }

    /**
     * Sets the grace period in milliseconds.
     *
     * @param gracePeriodInMillis The grace period to set, in milliseconds.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder gracePeriod(long gracePeriodInMillis) {
        this.gracePeriod = gracePeriodInMillis;
        return this;
    }

    /**
     * Sets the grace period.
     *
     * @param gracePeriod The grace period to set, in the provided time unit.
     * @param unit The time unit in which the grace period is given.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder gracePeriod(long gracePeriod, TimeUnit unit) {
        this.gracePeriod = unit.toMillis(gracePeriod);
        return this;
    }

    /**
     * Gets the grace period in milliseconds.
     *
     * @return the grace period
     */
    public long gracePeriod() {
        return this.gracePeriod;
    }

    /**
     * Gets the grace period in the provided time unit.
     *
     * @param unit The {@link TimeUnit} to which the Grace Period is converted
     * @return the grace period in the provided time unit
     */
    public long gracePeriod(TimeUnit unit) {
        return unit.convert(this.gracePeriod, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the cleanup interval in milliseconds.
     *
     * @param cleanupIntervalInMillis The cleanup interval to set, in milliseconds.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder cleanupInterval(long cleanupIntervalInMillis) {
        this.cleanupInterval = cleanupIntervalInMillis;
        return this;
    }

    /**
     * Sets the cleanup interval.
     *
     * @param cleanupInterval The cleanup interval to set, in the provided time unit.
     * @param unit The time unit in which the cleanup interval is given.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder cleanupInterval(long cleanupInterval, TimeUnit unit) {
        this.cleanupInterval = unit.toMillis(cleanupInterval);
        return this;
    }

    /**
     * Gets the cleanup interval in milliseconds.
     *
     * @return the cleanup interval
     */
    public long cleanupInterval() {
        return this.cleanupInterval;
    }

    /**
     * Gets the cleanup interval in the provided time unit.
     *
     * @param unit The {@link TimeUnit} to which the cleanup interval is converted
     * @return the cleanup interval in the provided time unit
     */
    public long cleanupInterval(TimeUnit unit) {
        return unit.convert(this.cleanupInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Constructs a Bloom filter using the specified parameters and computing missing parameters if possible (e.g. the
     * optimal Bloom filter bit size).
     *
     * @param <T> the type of element contained in the Bloom filter.
     * @return the constructed Bloom filter
     */
    public <T> BloomFilter<T> buildBloomFilter() {
        complete();
        if (redisBacked) {
            return new BloomFilterRedis<>(this);
        } else {
            return new BloomFilterMemory<>(this);
        }
    }

    /**
     * Constructs a Counting Bloom filter using the specified parameters and by computing missing parameters if possible
     * (e.g. the optimal Bloom filter bit size).
     *
     * @param <T> the type of element contained in the Counting Bloom filter.
     * @return the constructed Counting Bloom filter
     */
    public <T> CountingBloomFilter<T> buildCountingBloomFilter() {
        complete();
        if (redisBacked) {
            return new CountingBloomFilterRedis<>(this);
        } else {
            if (countingBits == 32) {
                return new CountingBloomFilter32<>(this);
            } else if (countingBits == 16) {
                return new CountingBloomFilter16<>(this);
            } else if (countingBits == 8) {
                return new CountingBloomFilter8<>(this);
            } else if (countingBits == 64) {
                return new CountingBloomFilter64<>(this);
            } else {
                return new CountingBloomFilterMemory<>(this);
            }
        }
    }

    /**
     * Checks if all necessary parameters were set and tries to infer optimal parameters (e.g. size and hashes from
     * given expectedElements and falsePositiveProbability). This is done automatically.
     *
     * @return the completed FilterBuilder
     */
    public FilterBuilder complete() {
        if (done) { return this; }
        if (size == null && expectedElements != null && falsePositiveProbability != null) {
            size = optimalM(expectedElements, falsePositiveProbability);
        }
        if (hashes == null && expectedElements != null && size != null) { hashes = optimalK(expectedElements, size); }
        if (size == null || hashes == null) {
            throw new NullPointerException("Neither (expectedElements, falsePositiveProbability) nor (size, hashes) were specified.");
        }
        if (expectedElements == null) { expectedElements = optimalN(hashes, size); }
        if (falsePositiveProbability == null) { falsePositiveProbability = optimalP(hashes, size, expectedElements); }

        done = true;
        return this;
    }


    @Override
    public FilterBuilder clone() {
        Object clone;
        try {
            clone = super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Cloning failed.");
        }
        return (FilterBuilder) clone;
    }


    /**
     * @return {@code true} if the Bloom Filter will be Redis-backed
     */
    public boolean redisBacked() {
        return redisBacked;
    }

    /**
     * @return the number of expected elements for the Bloom filter
     */
    public int expectedElements() {
        return expectedElements;
    }

    /**
     * @return the size of the Bloom filter in bits
     */
    public int size() {
        return size;
    }

    /**
     * @return the number of hashes used by the Bloom filter
     */
    public int hashes() {
        return hashes;
    }

    /**
     * @return The number of bits used for counting in case of a counting Bloom filter
     */
    public int countingBits() {
        return countingBits;
    }

    /**
     * @return the tolerable false positive probability of the Bloom filter
     */
    public double falsePositiveProbability() {
        return falsePositiveProbability;
    }

    /**
     * @return the name of the Bloom filter
     */
    public String name() {
        return name;
    }

    /**
     * @return the host name of the Redis server backing the Bloom filter
     */
    public String redisHost() {
        return redisHost;
    }

    /**
     * @return the port used by the Redis server backing the Bloom filter
     */
    public int redisPort() {
        return redisPort;
    }

    /**
     * @return the number of connections used by the Redis Server backing the Bloom filter
     */
    public int redisConnections() {
        return redisConnections;
    }

    /**
     * @return if SSL is enabled for Redis connection
     */
    public boolean redisSsl() {
        return redisSsl;
    }

    /**
     * @return The hash method to be used by the Bloom filter
     */
    public HashMethod hashMethod() {
        return hashMethod;
    }

    /**
     * @return the actual hash function to be used by the Bloom filter
     */
    public HashFunction hashFunction() {
        return hashFunction;
    }

    /**
     * @return Return the default Charset used for conversion of String values into byte arrays used for hashing
     */
    public static Charset defaultCharset() {
        return defaultCharset;
    }

    /**
     * @return {@code true} if the Bloom filter that is to be built should overwrite any existing Bloom filter with the
     * same name
     */
    public boolean overwriteIfExists() {
        return overwriteIfExists;
    }

    /**
     * @return return the list of all read slaves to be used by the Redis-backed Bloom filter
     */
    public Set<Entry<String, Integer>> getReadSlaves() {
        return slaves;
    }

    /**
     * Checks whether a configuration is compatible to another configuration based on the size of the Bloom filter and
     * its hash functions.
     *
     * @param other the other configuration
     * @return {@code true} if the configurations are compatible
     */
    public boolean isCompatibleTo(FilterBuilder other) {
        return this.size() == other.size() && this.hashes() == other.hashes() && this.hashMethod() == other.hashMethod();
    }

    /**
     * Calculates the optimal size <i>size</i> of the bloom filter in bits given <i>expectedElements</i> (expected
     * number of elements in bloom filter) and <i>falsePositiveProbability</i> (tolerable false positive rate).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param p Tolerable false positive rate
     * @return the optimal size <i>size</i> of the bloom filter in bits
     */
    public static int optimalM(long n, double p) {
        return (int) Math.ceil(-1 * (n * Math.log(p)) / Math.pow(Math.log(2), 2));
    }

    /**
     * Calculates the optimal <i>hashes</i> (number of hash function) given <i>expectedElements</i> (expected number of
     * elements in bloom filter) and <i>size</i> (size of bloom filter in bits).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param m The size of the bloom filter in bits.
     * @return the optimal amount of hash functions hashes
     */
    public static int optimalK(long n, long m) {
        return (int) Math.ceil((Math.log(2) * m) / n);
    }

    /**
     * Calculates the amount of elements a Bloom filter for which the given configuration of size and hashes is
     * optimal.
     *
     * @param k number of hashes
     * @param m The size of the bloom filter in bits.
     * @return amount of elements a Bloom filter for which the given configuration of size and hashes is optimal.
     */
    public static int optimalN(long k, long m) {
        return (int) Math.ceil((Math.log(2) * m) / k);
    }

    /**
     * Calculates the best-case (uniform hash function) false positive probability.
     *
     * @param k                number of hashes
     * @param m                The size of the bloom filter in bits.
     * @param insertedElements number of elements inserted in the filter
     * @return The calculated false positive probability
     */
    public static double optimalP(long k, long m, double insertedElements) {
        return Math.pow((1 - Math.exp(-k * insertedElements / (double) m)), k);
    }


    public String password() {
        return password;
    }

    public RedisPool pool() {
        if(done && pool == null) {
            pool = RedisPool.builder()
                .host(redisHost())
                .port(redisPort())
                .readSlaves(getReadSlaves())
                .password(password())
                .database(database())
                .redisConnections(redisConnections())
                .build();
        }
        return pool;
    }
}
