package orestes.bloomfilter.redis;

import orestes.bloomfilter.CBloomFilter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * This Redis-backed bloomfilter stores the counting bits in a bit array and uses Lua scripting (available in Redis 2.6
 * and above) to access it efficiently.
 * 
 * @param <T>
 */
public class CBloomFilterRedisBits<T> extends CBloomFilter<T> {
    private String incr;
	private String decr;
	private Jedis jedis;

    private BloomFilterRedisKeyProvider keyProvider;

	public CBloomFilterRedisBits(BloomFilterRedisKeyProvider keyProvider, String host, int port, double n, double p, int c) {
		this(keyProvider, host, port, optimalM(n, p), optimalK(n, optimalM(n, p)), c);
	}

	public CBloomFilterRedisBits(BloomFilterRedisKeyProvider keyProvider, String host, int port, int m, int k, int c) {
        super(new RedisBitSet(host, port, keyProvider.getCountsKey(), m * c), new RedisBitSet(host, port, keyProvider.getBloomKey(), m), m, k, c);

        this.keyProvider = keyProvider;
		this.jedis = new Jedis(host, port);
		// Create the bloomfilter metadata in Redis. If it is done concurrently --> abort
		jedis.watch(keyProvider.getMKey());
		if (!jedis.exists(keyProvider.getMKey())) {
			Transaction t = jedis.multi();
			t.set(keyProvider.getMKey(), Integer.toString(m));
			t.set(keyProvider.getKKey(), Integer.toString(k));
			t.set(keyProvider.getCKey(), Integer.toString(c));
			t.set(keyProvider.getHashKey(), getCryptographicHashFunctionName());
			t.exec();
		}
		incr = jedis.scriptLoad(SETANDINCR);
		decr = jedis.scriptLoad(SETANDDECR);
	}
	
	public CBloomFilterRedisBits(BloomFilterRedisKeyProvider keyProvider, Jedis jedis) {
		this(keyProvider, jedis.getClient().getHost(), jedis.getClient().getPort(), Integer.parseInt(jedis.get(keyProvider.getMKey())), Integer
				.parseInt(jedis.get(keyProvider.getKKey())), Integer.parseInt(jedis.get(keyProvider.getCKey())));
		setCryptographicHashFunction(jedis.get(keyProvider.getHashKey()));
	}

    public BloomFilterRedisKeyProvider getKeyProvider() {
        return keyProvider;
    }

    @Override
	public void remove(byte[] value) {
		lua(decr, hash(value));
	}

	@Override
	public boolean add(byte[] value) {
		lua(incr, hash(value));
		return true;
	}

	protected void lua(String script, int[] indexes) {
		String[] params = new String[4 + indexes.length];
		params[0] = getCounts().getRedisKey();
		params[1] = getBloom().getRedisKey();
		params[2] = String.valueOf(c);
		params[3] = String.valueOf(indexes.length);
		for (int i = 0; i < indexes.length; i++) {
			params[i + 4] = String.valueOf(indexes[i]);
		}
		jedis.evalsha(script, 2, params);
	}
	
	@Override
	public boolean contains(byte[] value) {
		return getBloom().allSet(hash(value));
	}

	protected RedisBitSet getBloom() {
		return (RedisBitSet) bloom;
	}

	protected RedisBitSet getCounts() {
		return (RedisBitSet) counts;
	}

	// countskey, bloomkey, c, indexcount, index1, index2, ...
	public final static String SETANDINCR = "for index = 3, ARGV[2] + 2, 1 do\r\n"
			+ "	redis.call('setbit', KEYS[2], ARGV[index], 1)\r\n" + "	local low = ARGV[index] * ARGV[1]\r\n"
			+ "	local high = (ARGV[index] + 1) * ARGV[1]\r\n" + "	local incremented = false\r\n"
			+ "	for i = (high - 1), low, -1 do\r\n" + "		if redis.call('getbit', KEYS[1], i) == 0 then\r\n"
			+ "			redis.call('setbit', KEYS[1], i, 1)\r\n" + "			incremented = true\r\n" + "			break\r\n"
			+ "		else\r\n" + "			redis.call('setbit', KEYS[1], i, 0)\r\n" + "		end\r\n" + "	end\r\n" + "end";

	// countskey, bloomkey, c, indexcount, index1, index2, ...
	public final static String SETANDDECR = "for index = 3, ARGV[2] + 2, 1 do\r\n"
			+ "	local low = ARGV[index] * ARGV[1]\r\n" + "	local high = (ARGV[index] + 1) * ARGV[1]\r\n"
			+ "	local decremented = false\r\n" + "	local nonZero = false;\r\n" + "	for i = (high - 1), low, -1 do\r\n"
			+ "		if not decremented then\r\n" + "			if redis.call('getbit', KEYS[1], i) == 1 then\r\n"
			+ "				redis.call('setbit', KEYS[1], i, 0)\r\n" + "				decremented = true\r\n" + "			else\r\n"
			+ "				redis.call('setbit', KEYS[1], i, 1)\r\n" + "				nonZero = true\r\n" + "			end\r\n" + "		else\r\n"
			+ "			if redis.call('getbit', KEYS[1], i) == 1 then\r\n" + "				nonZero = true\r\n" + "			end\r\n"
			+ "		end\r\n" + "	end\r\n" + "	if not nonZero then\r\n"
			+ "		redis.call('setbit', KEYS[2], ARGV[index], 0)\r\n" + "	end\r\n" + "end";

}
