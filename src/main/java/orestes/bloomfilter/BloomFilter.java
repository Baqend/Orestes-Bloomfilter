package orestes.bloomfilter;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class BloomFilter<T> implements Cloneable, Serializable {
	protected BitSet bloom;
	protected transient MessageDigest hashFunction;
	protected String hashFunctionName;
	protected int k;
	protected int m;
	private transient Charset defaultCharset;
	protected String defaultCharsetName = "UTF-8";
	protected HashMethod hashMethod = HashMethod.Cryptographic;
	private CustomHashFunction customHashFunction;
	protected final static int seed32 = 89478583;

    protected long population = 0;

	/**
	 * Calculates the optimal size <i>m</i> of the bloom filter in bits given
	 * <i>n</i> (expected number of elements in bloom filter) and <i>p</i>
	 * (tolerable false positive rate).
	 * 
	 * @param n
	 *            Expected number of elements inserted in the bloom filter
	 * @param p
	 *            Tolerable false positive rate
	 * @return the optimal size <i>m</i> of the bloom filter in bits
	 */
	public static int optimalM(double n, double p) {
		return (int) Math.ceil(-1 * n * Math.log(p) / Math.pow(Math.log(2), 2));
	}

	/**
	 * Calculates the optimal <i>k</i> (number of hash function) given <i>n</i>
	 * (expected number of elements in bloom filter) and <i>m</i> (size of bloom
	 * filter in bits).
	 * 
	 * @param n
	 *            Expected number of elements inserted in the bloom filter
	 * @param m
	 *            The size of the bloom filter in bits.
	 * @return the optimal amount of hash functions k
	 */
	public static int optimalK(double n, int m) {
		return (int) Math.ceil(Math.log(2) * m / n);
	}

	/**
	 * Constructs a new bloom filter by determining the optimal bloom filter
	 * size <i>n</i> in bits and the number of hash functions <i>k</i> based on
	 * the expected number <i>n</i> of elements in the bloom filter and the
	 * tolerable false positive rate <i>p</i>.
	 * 
	 * @param n
	 *            Expected number of elements inserted in the bloom filter
	 * @param p
	 *            Tolerable false positive rate
	 */
	public BloomFilter(double n, double p) {
		this(optimalM(n, p), optimalK(n, optimalM(n, p)));
	}

	/**
	 * Constructs a new bloom filter of the size <i>m</i> bits and <i>k</i> hash
	 * functions. You can calculate the optimal parameters using the static
	 * methods {@link #optimalK(double, int)} and/or
	 * {@link #optimalM(double, double)}, depending on wether you want to
	 * provide the tolerable false positive probability or the exact size of the
	 * bloom filter.
	 * 
	 * @param m
	 *            The size of the bloom filter in bits.
	 * @param k
	 *            The number of hash functions to use.
	 */
	public BloomFilter(int m, int k) {
		this(new BitSet(m), m, k);
	}

	
	protected BloomFilter(BitSet bloom, int m, int k) {
		this.m = m;
		this.bloom = bloom;
		this.k = k;
		setCryptographicHashFunction("MD5");
	}
	
	/**
	 * For lazy initialization in subclasses.
	 */
	protected BloomFilter() {
		
	}

	/**
	 * Constructs a new bloom filter by using the provided bit vector
	 * <i>bloomFilter</i>.
	 * 
	 * @param bloomFilter
	 *            the bit vector used to construct the bloom filter
	 * @param k
	 *            the number of hash functions used
	 * @param hashMethod
	 *            hash function type
	 * @param hashFunctionName
	 *            name of the hash function to be used for the cryptographic
	 *            HashMethod, i.e. MD2, MD5, SHA-1, SHA-256, SHA-384 or SHA-512
	 */
	public BloomFilter(BitSet bloomFilter, int m, int k, HashMethod hashMethod,
			String hashFunctionName) {
		this(bloomFilter, m, k, hashMethod, hashFunctionName, 0);
	}

    /**
     * Constructs a new bloom filter by using the provided bit vector
     * <i>bloomFilter</i>.
     *
     * @param bloomFilter
     *            the bit vector used to construct the bloom filter
     * @param k
     *            the number of hash functions used
     * @param hashMethod
     *            hash function type
     * @param hashFunctionName
     *            name of the hash function to be used for the cryptographic
     *            HashMethod, i.e. MD2, MD5, SHA-1, SHA-256, SHA-384 or SHA-512
     *
     * @param population
     *            the current population count for the provided filter
     */
    public BloomFilter(BitSet bloomFilter, int m, int k, HashMethod hashMethod,
                       String hashFunctionName, long population) {
        this.bloom = bloomFilter;
        this.k = k;
        this.m = m;
        this.population = population;
        setHashMethod(hashMethod);
        setCryptographicHashFunction(hashFunctionName);
    }

	/**
	 * Sets the method used to generate hash values. Note that all previously
	 * inserted elements become invalid if you change this setting and the bloom
	 * filter should be reset using {@link #clear()} before being used again.
	 * Possible hash methods are: <br>
	 * <ul>
	 * <li></li><tt>HashMethod.Cryptographic</tt> is the default. It uses a
	 * cryptographic hash function (e.g. MD5, SHA-512, default is MD5) which can be set using
	 * {@link #setCryptographicHashFunction(String)}. It slices the digest in
	 * bit ranges of x with 2^x > m and does rejection sampling for each slice.
	 * It is fast and very well distributed.</li>
	 * <li><tt>HashMethod.RNG</tt> uses the random number generator of Java,
	 * which is a Linear Congruent Generator (LCG). It is very fast but doesn't
	 * distribute hash values very uniformly.</li>
	 * <li><tt>HashMethod.SecureRNG</tt> uses the secure random number generator
	 * of Java, which satisfies a high degree of randomness at the cost of CPU
	 * time.</li>
	 * <li></li><tt>HashMethod.CarterWegman</tt> implements the Carter Wegman
	 * Universal Hashing Function. It has very good theoretical guarantees for
	 * the uniform distribution of hash values but can be ~10 times slower than
	 * other methods, as it has to perform arithmetic operations on potentially
	 * large numbers.
	 * <li><tt>HashMethod.CRC32</tt> uses a CRC32 checksum. It is rather fast an
	 * offers a good distribution.</li>
	 * <li><tt>HashMethod.Adler32</tt> uses the Adler32 checksum. Adler32 is
	 * faster than CRC32 but offers a slightly less uniform distribution.</li>
	 * <li><tt>HashMethod.Murmur</tt> uses the Murmur 2 hash function. It is a
	 * new and increasingly popular hash function that is fast and very
	 * uniformly distributed.</li>
	 * </ul>
	 * 
	 * @param hashMethod
	 *            the method used to generate hash values
	 */
	public void setHashMethod(HashMethod hashMethod) {
		this.hashMethod = hashMethod;
        this.hashFunctionName = hashMethod.toString();
	}

	/**
	 * Uses the given cryptographic hash function. Note that all previously
	 * inserted elements become invalid and the bloom filter should be reset
	 * using {@link #clear()} before being used again.
	 * 
	 * @param hashFunctionName
	 *            name of the hash function to be used, i.e. MD2, MD5, SHA-1,
	 *            SHA-256, SHA-384 or SHA-512
	 */
	public void setCryptographicHashFunction(String hashFunctionName) {
		this.hashMethod = HashMethod.Cryptographic;
		this.hashFunctionName = hashFunctionName;
		try {
			hashFunction = MessageDigest.getInstance(hashFunctionName);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(
					"Unknown hash function provided. Use  MD2, MD5, SHA-1, SHA-256, SHA-384 or SHA-512.");
		}
	}

	/**
	 * Gets {@link MessageDigest}
	 *
	 * @return Digest being used
	 */
	public MessageDigest getCryptographicHashFunction() {

        if (this.hashFunction == null) {
            setCryptographicHashFunction(hashFunctionName);
        }

        return this.hashFunction;
	}

	/**
	 * Uses a given custom hash function. Note that all previously inserted
	 * elements become invalid and the bloom filter should be reset using
	 * {@link #clear()} before being used again.
	 * 
	 * @param chf
	 *            the custom hash function
	 */
	public void setCustomHashFunction(CustomHashFunction chf) {
		this.hashMethod = HashMethod.Custom;
		this.customHashFunction = chf;
	}

    /**
     * Add the passed value to the filter.
     * @param value   key to add
     * @return true if the value does not exist in the filter. Note a false positive may occur, thus the value may
     *      not have already been in the filter, but it hashed to a set of bits already in the filter
     *
     */
	public boolean add(byte[] value) {
        boolean added = false;
		for (int position : hash(value)) {
            if (!getBit(position)) {
                added = true;
			    setBit(position);
            }
		}
        if (added) population++;
		return added;
	}

	public boolean add(T value) {
		return add(value.toString().getBytes(getDefaultCharset()));
	}

	public List<Boolean> addAll(Collection<T> values) {
        List<Boolean> added = new ArrayList<>();
		for (T value : values)
			added.add (add(value));
        return added;
	}

	public void clear() {
		bloom.clear();
        population = 0;
	}

	public boolean contains(byte[] value) {
		for (int position : hash(value))
			if (!getBit(position))
				return false;
		return true;
	}

	public boolean contains(T value) {
		return contains(value.toString().getBytes(getDefaultCharset()));
	}

    public List<Boolean> contains(Collection<T> values) {
        List<Boolean> does = new ArrayList<>();
        for (T value: values) {
            does.add(contains(value));
        }
        return does;
    }

	public boolean containsAll(Collection<T> values) {
		for (T value : values)
			if (!contains(value))
				return false;
		return true;
	}

	protected boolean getBit(int index) {
		return bloom.get(index);
	}

	/**
	 * Gets {@link #defaultCharset}
	 *
	 * @return default Charset for encoding the bits
	 */
	protected Charset getDefaultCharset() {

        if (this.defaultCharset == null) {
            this.defaultCharset = Charset.forName(defaultCharsetName);
        }

        return this.defaultCharset;
	}

	protected void setBit(int index) {
		setBit(index, true);
	}

	protected void setBit(int index, boolean to) {
		bloom.set(index, to);
	}

	public BitSet getBitSet() {
		return bloom;
	}

	/**
	 * Dispatches the hash function defines via
	 * {@link #setHashMethod(HashMethod)} (default: cryptographic hash
	 * function).
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	public int[] hash(String value) {
		return hash(value.getBytes(getDefaultCharset()));
	}

	/**
	 * Uses the Fowler–Noll–Vo (FNV) hash function to generate a hash value from
	 * a byte array. It is superior to the standard implementation in
	 * {@link Arrays} and can be easily implemented in most languages.
	 * 
	 * @param a
	 *            the byte array to be hashed
	 * @return the 32 bit integer hash value
	 */
	protected static int hashBytes(byte a[]) {
		// 32 bit FNV constants. Using longs as Java does not support unsigned
		// datatypes.
		final long FNV_PRIME = 16777619;
		final long FNV_OFFSET_BASIS = 2166136261l;

		if (a == null)
			return 0;

		long result = FNV_OFFSET_BASIS;
		for (byte element : a) {
			result = (result * FNV_PRIME) & 0xFFFFFFFF;
			result ^= element;
		}

		// return Arrays.hashCode(a);
		return (int) result;
	}

	/**
	 * Generates hash values using the Carter Wegman function (<a href="http://en.wikipedia.org/wiki/Universal_hashing}">Wikipedia</a>), which is a universal
	 * hashing function. It thus has optimal guarantees for the uniformity of
	 * generated hash values. On the downside, the performance is not optimal,
	 * as arithmetic operations on large numbers have to be performed.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashCarterWegman(byte[] value) {
		int[] positions = new int[k];
		BigInteger prime32 = BigInteger.valueOf(4294967279l);
		BigInteger prime64 = BigInteger.valueOf(53200200938189l);
		BigInteger prime128 = new BigInteger("21213943449988109084994671");
		Random r = getRandom();
		// BigInteger v = new BigInt(value).toBigInteger();
		BigInteger v = BigInteger.valueOf(hashBytes(value));

		for (int i = 0; i < k; i++) {
			BigInteger a = BigInteger.valueOf(r.nextLong());
			BigInteger b = BigInteger.valueOf(r.nextLong());
			positions[i] = a.multiply(v).add(b).mod(prime64)
					.mod(BigInteger.valueOf(m)).intValue();
		}
		return positions;
	}

	/**
	 * Generates a hash value using the Java Random Number Generator (RNG) which
	 * is a Linear Congruential Generator (LCG), implementing the following
	 * formula: <br/>
	 * <code>number_i+1 = (a * number_i + c) mod m</code><br/>
	 * <br/>
	 * The RNG is initialized using the value to be hashed.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashRNG(byte[] value) {
		int[] positions = new int[k];
		Random r = new Random(hashBytes(value));
		for (int i = 0; i < k; i++) {
			positions[i] = r.nextInt(m);
		}
		return positions;
	}

	/**
	 * Generates a hash value using the Secure Java Random Number Generator
	 * (RNG). It is more random than the normal RNG but more CPU intensive. The
	 * RNG is initialized using the value to be hashed.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashSecureRNG(byte[] value) {
		int[] positions = new int[k];
		SecureRandom r = new SecureRandom(value);
		for (int i = 0; i < k; i++) {
			positions[i] = r.nextInt(m);
		}
		return positions;
	}

	protected int[] hashMagnus(byte[] value) {
		int hashes = k;
		int[] result = new int[hashes];

		int k = 0;
		byte salt = 0;

		while (k < hashes) {
			byte[] digest;
			synchronized (hashFunction) {
				hashFunction.update(salt);
				salt++;
				digest = hashFunction.digest(value);
			}

			for (int i = 0; i < digest.length / 4 && k < hashes; i++) {
				int h = 0;
				for (int j = (i * 4); j < (i * 4) + 4; j++) {
					h <<= 8;
					h |= ((int) digest[j]) & 0xFF;
				}
				result[k] = Math.abs(h % m);
				k++;
			}
		}
		return result;
	}

	/**
	 * Generates a hash value using a Cyclic Redundancy Check (CRC32). CRC is
	 * designed as a checksum for data integrity not as hash function but
	 * exhibits very good uniformity and is relatively fast.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashCRC(byte[] value) {
		return hashChecksum(value, new CRC32());
	}

	/**
	 * Generates a hash value using the Adler32 Checksum algorithm. Adler32 is
	 * comaprable to CRC32 but is faster at the cost of a less uniform
	 * distribution of hash values.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashAdler(byte[] value) {
		return hashChecksum(value, new Adler32());
	}

	protected int[] hashChecksum(byte[] value, Checksum cs) {
		int[] positions = new int[k];
		int hashes = 0;
		int salt = 0;
		while (hashes < k) {
			cs.reset();
			cs.update(value, 0, value.length);
			// Modify the data to be checksummed by adding the number of already
			// calculated hashes, the loop counter and
			// a static seed
			cs.update(hashes + salt++ + seed32);
			int hash = rejectionSample((int) cs.getValue());
			if (hash != -1) {
				positions[hashes++] = hash;
			}
		}
		return positions;
	}

	/**
	 * Uses the very simple LCG scheme and the Java initialization constants.
	 * This method is intended to be employed if the bloom filter has to be used
	 * in a language which doesn't support any of the other hash functions. This
	 * hash function can then easily be implemented.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashLCG(byte[] value) {
		// Java constants
		final long multiplier = 0x5DEECE66DL;
		final long addend = 0xBL;
		final long mask = (1L << 48) - 1;

		// Generate int from byte Array using the FNV hash
		int reduced = Math.abs(hashBytes(value));
		// Make number positive
		// Handle the special case: smallest negative number is itself as the
		// absolute value
		if (reduced == Integer.MIN_VALUE)
			reduced = 42;

		// Calculate k numbers iterativeley
		int[] positions = new int[k];
		long seed = reduced;
		for (int i = 0; i < k; i++) {
			// LCG formula: x_i+1 = (multiplier * x_i + addend) mod mask
			seed = (seed * multiplier + addend) & mask;
			positions[i] = (int) (seed >>> (48 - 30)) % m;
		}
		return positions;
	}

	protected int[] hashMurmur(byte[] value) {
		int[] positions = new int[k];

		int hashes = 0;
		int lastHash = 0;
		byte[] data = (byte[]) value.clone();
		while (hashes < k) {
			// Code taken from:
			// http://dmy999.com/article/50/murmurhash-2-java-port by Derekt
			// Young (Public Domain)
			// as the Hadoop implementation by Andrzej Bialecki is buggy

			for (int i = 0; i < value.length; i++) {
				if (data[i] == 127) {
					data[i] = 0;
					continue;
				} else {
					data[i]++;
					break;
				}
			}

			// 'm' and 'r' are mixing constants generated offline.
			// They're not really 'magic', they just happen to work well.
			int m = 0x5bd1e995;
			int r = 24;

			// Initialize the hash to a 'random' value
			int len = data.length;
			int h = seed32 ^ len;

			int i = 0;
			while (len >= 4) {
				int k = data[i + 0] & 0xFF;
				k |= (data[i + 1] & 0xFF) << 8;
				k |= (data[i + 2] & 0xFF) << 16;
				k |= (data[i + 3] & 0xFF) << 24;

				k *= m;
				k ^= k >>> r;
				k *= m;

				h *= m;
				h ^= k;

				i += 4;
				len -= 4;
			}

			switch (len) {
			case 3:
				h ^= (data[i + 2] & 0xFF) << 16;
			case 2:
				h ^= (data[i + 1] & 0xFF) << 8;
			case 1:
				h ^= (data[i + 0] & 0xFF);
				h *= m;
			}

			h ^= h >>> 13;
			h *= m;
			h ^= h >>> 15;

			lastHash = rejectionSample(h);
			if (lastHash != -1) {
				positions[hashes++] = lastHash;
			}
		}
		return positions;
	}

	/**
	 * Performs rejection sampling on a random 32bit Java int (sampled from
	 * Integer.MIN_VALUE to Integer.MAX_VALUE).
	 * 
	 * @param random
	 *            int
	 * @return the number down-sampled to interval [0, m]. Or -1 if it has to be
	 *         rejected.
	 */
	protected int rejectionSample(int random) {
		random = Math.abs(random);
		if (random > (2147483647 - 2147483647 % m)
				|| random == Integer.MIN_VALUE)
			return -1;
		else
			return random % m;
	}

	/**
	 * Generates a hash value using a cryptographic hash function supplied by
	 * {@link #setCryptographicHashFunction(String)} (default is MD5). This
	 * method is moderately fast and has good guarantees for the uniformity of
	 * generated hash values, as the hash functions are designed for
	 * cryptographic use.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hashCrypt(byte[] value) {
		int[] positions = new int[k];

		int computedHashes = 0;
		// Add salt to the hash deterministically in order to generate different
		// hashes for each round
		// Alternative: use pseudorandom sequence
		Random r = getRandom();
		byte[] digest = new byte[0];
		while (computedHashes < k) {
			// byte[] saltBytes =
			// ByteBuffer.allocate(4).putInt(r.nextInt()).array();
			getCryptographicHashFunction().update(digest);
			digest = getCryptographicHashFunction().digest(value);
			BitSet hashed = BitSet.valueOf(digest);

			// Convert the hash to numbers in the range [0,m)
			// Size of the BloomFilter rounded to the next power of two
			int filterSize = 32 - Integer.numberOfLeadingZeros(m);
			// Computed hash bits
			int hashBits = digest.length * 8;
			// Split the hash value according to the size of the Bloomfilter
			for (int split = 0; split < (hashBits / filterSize)
					&& computedHashes < k; split++) {
				int from = split * filterSize;
				int to = (split + 1) * filterSize;
				BitSet hashSlice = hashed.get(from, to);
				// Bitset to Int
				long[] longHash = hashSlice.toLongArray();
				int intHash = longHash.length > 0 ? (int) longHash[0] : 0;
				// Only use the position if it's in [0,m); Called rejection
				// sampling
				if (intHash < m) {
					positions[computedHashes] = intHash;
					computedHashes++;
				}
			}
		}

		return positions;
	}

	/**
	 * Calls the hash function defined through
	 * {@link #setHashMethod(HashMethod)}.
	 * 
	 * @param value
	 *            the value to be hashed
	 * @return array with <i>k</i> integer hash positions in the range
	 *         <i>[0,m)</i>
	 */
	protected int[] hash(byte[] value) {
		// Dispatch the chosen hash method
		switch (hashMethod) {
		case RNG:
			return hashRNG(value);
		case SecureRNG:
			return hashSecureRNG(value);
		case CarterWegman:
			return hashCarterWegman(value);
		case CRC32:
			return hashCRC(value);
		case Adler32:
			return hashAdler(value);
		case Murmur:
			return hashMurmur(value);
		case SimpeLCG:
			return hashLCG(value);
		case Magnus:
			return hashMagnus(value);
		case Custom:
			return customHashFunction.hash(value, m, k);
		default:
			return hashCrypt(value);
		}
	}

	/**
	 * Performs the union operation on two compatible bloom filters. This is
	 * achieved through a bitwise OR operation on their bit vectors. This
	 * operations is lossless, i.e. no elements are lost and the bloom filter is
	 * the same that would have resulted if all elements wer directly inserted
	 * in just one bloom filter.
	 * 
	 * @param other
	 *            the other bloom filter
	 * @return <tt>true</tt> if this bloom filter could successfully be updated
	 *         through the union with the provided bloom filter
	 */
	public synchronized boolean union(BloomFilter<T> other) {
		if (compatible(this, other)) {
			bloom.or(other.bloom);
			return true;
		}
		return false;
	}

	/**
	 * Performs the intersection operation on two compatible bloom filters. This
	 * is achieved through a bitwise AND operation on their bit vectors. The
	 * operations doesn't introduce any false negatives but it does raise the
	 * false positive probability. The the false positive probability in the
	 * resulting Bloom filter is at most the false-positive probability in one
	 * of the constituent bloom filters
	 * 
	 * @param other
	 *            the other bloom filter
	 * @return <tt>true</tt> if this bloom filter could successfully be updated
	 *         through the intersection with the provided bloom filter
	 */
	public synchronized boolean intersect(BloomFilter<T> other) {
		if (compatible(this, other)) {
			bloom.and(other.bloom);
			return true;
		}
		return false;
	}

	protected boolean compatible(BloomFilter<T> bloomFilter,
			BloomFilter<T> other) {
		return this.m == other.m && this.k == other.k
				&& this.hashFunctionName.equals(other.hashFunctionName)
				&& this.hashMethod == other.hashMethod;
	}

	public synchronized boolean isEmpty() {
		return bloom.isEmpty();
	}

	/**
	 * Returns the probability of a false positive (approximated): <br/>
	 * <code>(1 - e^(-k * insertedElements / m)) ^ k</code>
	 * 
	 * @param n
	 *            The number of elements already inserted into the Bloomfilter
	 * @return probability of a false positive after <i>n</i>
	 *         {@link #add(byte[])} operations
	 */
	public double getFalsePositiveProbability(int n) {
		return Math.pow((1 - Math.exp(-k * (double) n / (double) m)), k);
	}

	/**
	 * Calculates the numbers of Bits per element, based on the expected number
	 * of inserted elements <i>n</i>.
	 * 
	 * @param n
	 *            The number of elements already inserted into the Bloomfilter
	 * @return The numbers of bits per element
	 */
	public double getBitsPerElement(int n) {
		return m / (double) n;
	}

	/**
	 * Returns the probability that a bit is zero.
	 * 
	 * @param n
	 *            The number of elements already inserted into the Bloomfilter
	 * @return probability that a certain bit is zero after <i>n</i>
	 *         {@link #add(byte[])} operations
	 */
	public double getBitZeroProbability(int n) {
		return Math.pow(1 - (double) 1 / m, k * n);
	}

	/**
	 * Returns the size of the bloom filter.
	 * 
	 * @return Size of the bloom filter in bits
	 */
	public int size() {
		return m;
	}

	/**
	 * Returns the size of the bloom filter.
	 * 
	 * @return Size of the bloom filter in bits
	 */
	public int getM() {
		return m;
	}

	/**
	 * Returns the number of hash functions.
	 * 
	 * @return number of hash functions used
	 */
	public int getK() {
		return k;
	}

	/**
	 * Returns the name of the cryptographic hash function used when the hash
	 * method is <tt>HashMethod.Cryptographic</tt>.
	 * 
	 * @return the name of the cryptographic hash function used when the hash
	 *         method is <tt>HashMethod.Cryptographic</tt>
	 */
	public String getCryptographicHashFunctionName() {
		return hashFunctionName;
	}

	/**
	 * Returns the hash method used to calculate hash values.
	 * 
	 * @return the hash method used to calculate hash values.
	 */
	public HashMethod getHashMethod() {
		return hashMethod;
	}

	protected Random getRandom() {
		return new Random(160598551545387l);
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized Object clone() {
		BloomFilter<T> o = null;
		try {
			o = (BloomFilter<T>) super.clone();
		} catch (CloneNotSupportedException e) {
		}
		o.bloom = (BitSet) bloom.clone();
		o.setCryptographicHashFunction(this.hashFunctionName);
		o.k = k;
		o.m = m;
		return o;
	}

	@Override
	public synchronized int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bloom == null) ? 0 : bloom.hashCode());
		result = prime
				* result
				+ ((hashFunctionName == null) ? 0 : hashFunctionName.hashCode());
		result = prime * result + k;
		result = prime * result + m;
		return result;
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BloomFilter other = (BloomFilter) obj;
		if (bloom == null) {
			if (other.bloom != null)
				return false;
		} else if (!bloom.equals(other.bloom))
			return false;
		if (hashFunctionName == null) {
			if (other.hashFunctionName != null)
				return false;
		} else if (!hashFunctionName.equals(other.hashFunctionName))
			return false;
		if (k != other.k)
			return false;
		if (m != other.m)
			return false;
		return true;
	}

	@Override
	public synchronized String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Bloom Filter, Parameters ");
		sb.append("m = " + getM() + ", ");
		sb.append("k = " + getK() + ", ");
		for (int i = 0; i < m; i++) {
			sb.append(bloom.get(i) ? 1 : 0);
			sb.append("\n");
		}
		return sb.toString();
	}

	/**
	 * Different types of hash functions that can be used.
	 */
	public static enum HashMethod {
		RNG("RNG"), Cryptographic("Cryptographic"), CarterWegman("CarterWegman"), SecureRNG(
				"SecureRNG"), CRC32("CRC32"), Adler32("Adler32"), Murmur(
				"Murmur"), SimpeLCG("SimpleLCG"), Magnus("Magnus"), Custom(
				"Custom");

		private String name;

		private HashMethod(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * An interface which can be implemented to provide custom hash functions.
	 */
	public static interface CustomHashFunction extends Serializable {
		/**
		 * Computes hash values.
		 * 
		 * @param value
		 *            the byte[] representation of the element to be hashed
		 * @param m
		 *            integer output range [1,m]
		 * @param k
		 *            number of hashes to be computed
		 * @return int array of k hash values
		 */
		public int[] hash(byte[] value, int m, int k);
	}

    public long getPopulation() {
        return population;
    }

}
