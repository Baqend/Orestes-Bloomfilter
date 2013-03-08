package orestes.bloomfilter.redis;

import java.util.BitSet;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.util.SafeEncoder;

/**
 * A persistent BitSet backed by Redis. Not all methods of the superclass are implemented. If needed they can be used
 * converting the RedisBitSet to a regular BitSet by calling {@link #asBitSet()}. <br>
 * <br>
 * External transactions or pipeline can be propagated for use by modifying methods (e.g. {@link #set(int)}).
 * 
 */
public class RedisBitSet extends BitSet {
	private Jedis jedis;
	private String name;
	private int size;
	private Pipeline externalPipeline;
	private Transaction externalTransaction;

	/**
	 * Constructs an new RedisBitSet. It uses a new Redis connection with same host and port as the provided connection.
	 * 
	 * @param host
	 *            the Redis host
	 * @param port
	 *            the Redis Port
	 * @param name
	 *            the name used as key in the database
	 * @param size
	 *            the initial size of the RedisBitSet
	 */
	public RedisBitSet(String host, int port, String name, int size) {
		// Create own connection
		this.jedis = new Jedis(host, port);
		this.name = name;
		this.size = size;
		// Handle concurrent creation RedisBitSet backed by the Redis bitset
		jedis.watch(name);
		if (!jedis.exists(name)) {
			Transaction t = jedis.multi();
			t.setbit(name, size, false);
			t.exec();
		}
		jedis.unwatch();
	}

	/**
	 * Uses an external Redis pipeline context for all subsequent modifying operations (like {@link #set(int)}) until
	 * {@link #leaveContext()} is called.
	 * 
	 * @param p
	 *            The propagated Redis pipeline
	 */
	public void useContext(Pipeline p) {
		if (!isInContext()) {
			externalPipeline = p;
		} else {
			throw new RuntimeException("External context already set.");
		}
	}

	/**
	 * Uses an external Redis transaction context for all subsequent modifying operations (like {@link #set(int)}) until
	 * {@link #leaveContext()} is called.
	 * 
	 * @param t
	 *            The propagated Redis transaction
	 */
	public void useContext(Transaction t) {
		if (!isInContext()) {
			externalTransaction = t;
		} else {
			throw new RuntimeException("External context already set.");
		}
	}

	/**
	 * Leaves the external pipeline or transaction context.
	 */
	public void leaveContext() {
		externalPipeline = null;
		externalTransaction = null;
	}

	public boolean isInContext() {
		return externalPipeline != null || externalTransaction != null;
	}

	@Override
	public boolean get(int bitIndex) {
		return jedis.getbit(name, bitIndex);
	}

	/**
	 * Fetches the values at the given index positions in a multi transaction. This guarantees a consistent view.
	 * 
	 * @param indexes
	 *            the index positions to query
	 * @return an array containing the values at the given index positions
	 */
	public boolean[] getBulk(int... indexes) {
		boolean[] result = new boolean[indexes.length];
		Transaction t = jedis.multi();
		for (int index : indexes) {
			t.getbit(name, index);
		}
		int pos = 0;
		List<Object> tResult = t.exec();
		for (Object obj : tResult)
			result[pos++] = (Boolean) obj;
		return result;
	}

	@Override
	public void set(int bitIndex, boolean value) {
		if (!isInContext()) {
			jedis.setbit(name, bitIndex, value);
		} else if (externalPipeline != null) {
			externalPipeline.setbit(name, bitIndex, value);
		} else if (externalTransaction != null) {
			externalTransaction.setbit(name, bitIndex, value);
		}
	}

	/**
	 * Performs the normal {@link #set(int, boolean)} operation using the given transaction.
	 * 
	 * @param t
	 *            the propagated transaction
	 * @param bitIndex
	 *            a bit index
	 * @param value
	 *            a boolean value to set
	 */
	public void set(Transaction t, int bitIndex, boolean value) {
		t.setbit(name, bitIndex, value);
	}

	/**
	 * Performs the normal {@link #set(int, boolean)} operation using the given pipeline.
	 * 
	 * @param t
	 *            the propagated pipeline
	 * @param bitIndex
	 *            a bit index
	 * @param value
	 *            a boolean value to set
	 */
	public void set(Pipeline p, int bitIndex, boolean value) {
		p.setbit(name, bitIndex, value);
	}

	@Override
	public void set(int bitIndex) {
		set(bitIndex, true);
	}

	@Override
	public void clear(int bitIndex) {
		set(bitIndex, false);
	}

	@Override
	public void clear() {
		Transaction t = jedis.multi();
		t.del(name);
		t.setbit(name, size, false);
		t.exec();
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public byte[] toByteArray() {
		return jedis.get(SafeEncoder.encode(name));
	}

	/**
	 * Returns the RedisBitSet as a regular BitSet.
	 * 
	 * @return this RedisBitSet as a regular BitSet
	 */
	public BitSet asBitSet() {
		return BitSet.valueOf(toByteArray());
	}

	/**
	 * Overwrite the contents of this RedisBitSet by the given BitSet.
	 * 
	 * @param bitSet
	 *            a regular BitSet used to overwrite this RedisBitSet
	 */
	public void overwrite(BitSet bitSet) {
		jedis.set(SafeEncoder.encode(name), bitSet.toByteArray());
	}

	@Override
	public String toString() {
		return asBitSet().toString();
	}

	public String getRedisKey() {
		return name;
	}

	/**
	 * Tests whether the provided bit positions are all set.
	 * 
	 * @param positions
	 *            the positions to test
	 * @return <tt>true</tt> if all positions are set
	 */
	public boolean allSet(int... positions) {
		boolean[] results = getBulk(positions);
		for (boolean bit : results) {
			if (!bit)
				return false;
		}
		return true;
	}

}
