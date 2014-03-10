package orestes.bloomfilter.redis;

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
 * filter, e.g. {@link CBloomFilterRedis}. The performance of this datastructure is very good, as operations are grouped
 * into fast transactions, minimizing the network overhead of all bloom filter operations to one round trip to Redis.
 * 
 * @param <T>
 */
public class BloomFilterRedis<T> extends BloomFilter<T> {
	final static String BLOOM = "normalbloomfilter";
	final static String M = BLOOM + ":m";
	final static String K = BLOOM + ":k";
	final static String C = BLOOM + ":c";
	final static String HASH = BLOOM + ":hash";
	private Jedis jedis;
	private Pipeline p;

	public BloomFilterRedis(String host, int port, double n, double p) {
		this(host, port, optimalM(n, p), optimalK(n, optimalM(n, p)));
	}

	/**
	 * Creates a new persistent Bloomfilter backed by Redis.
	 * 
	 * @param host
	 *            the Redis host name or IP address
	 * @param port
	 *            the Redis port
	 * @param m
	 *            The size of the bloom filter in bits.
	 * @param k
	 *            The number of hash functions to use.
	 */
	public BloomFilterRedis(String host, int port, int m, int k) {
		super(new RedisBitSet(host, port, BLOOM, m), m, k);
		this.jedis = new Jedis(host, port);
		jedis.watch(M);
		if (!jedis.exists(M)) {
			Transaction t = jedis.multi();
			t.set(M, Integer.toString(m));
			t.set(K, Integer.toString(k));
			t.set(HASH, getCryptographicHashFunctionName());
			t.exec();
		}
	}

	public BloomFilterRedis(Jedis jedis) {
		this(jedis.getClient().getHost(), jedis.getClient().getPort(), Integer.parseInt(jedis.get(M)), Integer
				.parseInt(jedis.get(K)));
		setCryptographicHashFunction(jedis.get(HASH));
	}

	@Override
	public boolean add(byte[] value) {
		begin();
		super.add(value);
		if (commit() == null)
			return add(value);
		else
			return true;
	}

	@Override
	public void addAll(Collection<T> values) {
		begin();
		for (T val : values) {
			super.add(val.toString().getBytes(getDefaultCharset()));
		}
		if (commit() == null)
			addAll(values);
	}

	@Override
	public boolean contains(byte[] value) {
		return getBloom().allSet(hash(value));
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

}
