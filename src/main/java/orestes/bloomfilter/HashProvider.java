package orestes.bloomfilter;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 *
 */
public class HashProvider {
    private static final int seed32 = 89478583;


    /**
     * @param a the byte array to be hashed
     * @return the 32 bit integer hash value
     */
    static int hashBytes(byte a[]) {
        // 32 bit FNV constants. Using longs as Java does not support unsigned
        // datatypes.
        long FNV_PRIME = 16777619;
        long FNV_OFFSET_BASIS = 2166136261l;

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
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashCarterWegman(byte[] value, int m, int k) {
        int[] positions = new int[k];
        BigInteger prime32 = BigInteger.valueOf(4294967279l);
        BigInteger prime64 = BigInteger.valueOf(53200200938189l);
        BigInteger prime128 = new BigInteger("21213943449988109084994671");
        Random r = new Random(seed32);
        //BigInteger.valueOf(hashBytes(value)
        BigInteger v = new BigInteger(value.length > 0 ? value : new byte[1]);

        for (int i = 0; i < k; i++) {
            BigInteger a = BigInteger.valueOf(r.nextLong());
            BigInteger b = BigInteger.valueOf(r.nextLong());
            positions[i] = a.multiply(v).add(b).mod(prime64)
                    .mod(BigInteger.valueOf(m)).intValue();
        }
        return positions;
    }

    /**
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashRNG(byte[] value, int m, int k) {
        int[] positions = new int[k];
        Random r = new Random(hashBytes(value));
        for (int i = 0; i < k; i++) {
            positions[i] = r.nextInt(m);
        }
        return positions;
    }



    /**
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashCRC(byte[] value, int m, int k) {
        return hashChecksum(value, new CRC32(), m, k);
    }

    /**
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashAdler(byte[] value, int m, int k) {
        return hashChecksum(value, new Adler32(), m, k);
    }

    public static int[] hashChecksum(byte[] value, Checksum cs, int m, int k) {
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
            int hash = rejectionSample((int) cs.getValue(), m);
            if (hash != -1) {
                positions[hashes++] = hash;
            }
        }
        return positions;
    }

    /**
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashSimpleLCG(byte[] value, int m, int k) {
        // Java constants
        long multiplier = 0x5DEECE66DL;
        long addend = 0xBL;
        long mask = (1L << 48) - 1;

        // Generate int from byte Array using the FNV hash
        int reduced = Math.abs(hashBytes(value));
        // Make number positive
        // Handle the special case: smallest negative number is itself as the
        // absolute value
        if (reduced == Integer.MIN_VALUE)
            reduced = 42;

        // Calculate hashes numbers iteratively
        int[] positions = new int[k];
        long seed = reduced;
        for (int i = 0; i < k; i++) {
            // LCG formula: x_i+1 = (multiplier * x_i + addend) mod mask
            seed = (seed * multiplier + addend) & mask;
            positions[i] = (int) (seed >>> (48 - 30)) % m;
        }
        return positions;
    }

    public static int[] hashMurmur3(byte[] value, int m, int k) {
        return rejectionSample(HashProvider::murmur3_signed, value, m, k);
    }

    public static int[] hashCassandra(byte[] value, int m, int k) {
        int[] result = new int[k];
        long hash1 = murmur3(0, value);
        long hash2 = murmur3((int) hash1, value);
        for (int i = 0; i < k; i++) {
            result[i] = (int) ((hash1 + i * hash2) % m);
        }
        return result;
    }

    public static long murmur3(int seed, byte[] bytes) {
        return Integer.toUnsignedLong(murmur3_signed(seed, bytes));
    }

    public static int murmur3_signed(int seed, byte[] bytes) {
        int h1 = seed;
        //Standard in Guava
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int len = bytes.length;
        int i = 0;

        while (len >= 4) {
            //process()
            int k1  = (bytes[i++] & 0xFF);
                k1 |= (bytes[i++] & 0xFF) << 8;
                k1 |= (bytes[i++] & 0xFF) << 16;
                k1 |= (bytes[i++] & 0xFF) << 24;

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;

            len -= 4;
        }

        //processingRemaining()
        int k1 = 0;
        switch (len) {
            case 3:
                k1 ^= (bytes[i + 2] & 0xFF) << 16;
                // fall through
            case 2:
                k1 ^= (bytes[i + 1] & 0xFF) << 8;
                // fall through
            case 1:
                k1 ^= (bytes[i] & 0xFF);

                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }
        i += len;

        //makeHash()
        h1 ^= i;

        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }


    // Code taken from:
    // http://dmy999.com/article/50/murmurhash-2-java-port by Derekt
    // Young (Public Domain)
    // as the Hadoop implementation by Andrzej Bialecki is buggy
    public static int[] hashMurmur2(byte[] value, int em, int ka) {
        int[] positions = new int[ka];

        int hashes = 0;
        int lastHash = 0;
        byte[] data = value.clone();
        while (hashes < ka) {


            for (int i = 0; i < value.length; i++) {
                if (data[i] == 127) {
                    data[i] = 0;
                    continue;
                } else {
                    data[i]++;
                    break;
                }
            }

            // 'size' and 'r' are mixing constants generated offline.
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

            lastHash = rejectionSample(h, em);
            if (lastHash != -1) {
                positions[hashes++] = lastHash;
            }
        }
        return positions;
    }

    /**
     * Performs rejection sampling on a random 32bit Java int (sampled from Integer.MIN_VALUE to Integer.MAX_VALUE).
     *
     * @param random int
     * @param m     integer output range [1,size]
     * @return the number down-sampled to interval [0, size]. Or -1 if it has to be rejected.
     */
    public static int rejectionSample(int random, int m) {
        random = Math.abs(random);
        if (random > (2147483647 - 2147483647 % m)
                || random == Integer.MIN_VALUE)
            return -1;
        else
            return random % m;
    }

    public static int[] rejectionSample(BiFunction<Integer, byte[], Integer> hashFunction, byte[] value, int m, int k) {
        int[] hashes = new int[k];
        int seed = 0;
        int pos = 0;
        while (pos < k) {
            seed = hashFunction.apply(seed, value);
            int hash = rejectionSample(seed, m);
            if (hash != -1) {
                hashes[pos++] = hash;
            }
        }
        return hashes;
    }

    /**
     * @param value the value to be hashed
     * @param m     integer output range [1,size]
     * @param k     number of hashes to be computed
     * @param method the hash method name used by {@link MessageDigest#getInstance(String)}
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public static int[] hashCrypt(byte[] value, int m, int k, String method) {
        //MessageDigest is not thread-safe --> use new instance
        MessageDigest cryptHash = null;
        try {
            cryptHash = MessageDigest.getInstance(method);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        int[] positions = new int[k];

        int computedHashes = 0;
        // Add salt to the hash deterministically in order to generate different
        // hashes for each round
        // Alternative: use pseudorandom sequence
        Random r = new Random(seed32);
        byte[] digest = new byte[0];
        while (computedHashes < k) {
            // byte[] saltBytes =
            // ByteBuffer.allocate(4).putInt(r.nextInt()).array();
            cryptHash.update(digest);
            digest = cryptHash.digest(value);
            BitSet hashed = BitSet.valueOf(digest);

            // Convert the hash to numbers in the range [0,size)
            // Size of the BloomFilter rounded to the next power of two
            int filterSize = 32 - Integer.numberOfLeadingZeros(m);
            // Computed hash bits
            int hashBits = digest.length * 8;
            // Split the hash value according to the size of the Bloomfilter --> higher performance than just doing modulo
            for (int split = 0; split < (hashBits / filterSize)
                    && computedHashes < k; split++) {
                int from = split * filterSize;
                int to = (split + 1) * filterSize;
                BitSet hashSlice = hashed.get(from, to);
                // Bitset to Int
                long[] longHash = hashSlice.toLongArray();
                int intHash = longHash.length > 0 ? (int) longHash[0] : 0;
                // Only use the position if it's in [0,size); Called rejection sampling
                if (intHash < m) {
                    positions[computedHashes] = intHash;
                    computedHashes++;
                }
            }
        }

        return positions;
    }


    /**
     * An interface which can be implemented to provide custom hash functions.
     */
    public static interface HashFunction extends Serializable {
        
        /**
         * Computes hash values.
         *
         * @param value the byte[] representation of the element to be hashed
         * @param m     integer output range [1,size]
         * @param k     number of hashes to be computed
         * @return int array of hashes hash values
         */
        public int[] hash(byte[] value, int m, int k);
    }


    /**
     * Different types of hash functions that can be used.
     */
    public static enum HashMethod {
        /**
         * Generates hash values using the Java Random Number Generator (RNG) which is a Linear Congruential Generator
         * (LCG), implementing the following formula: <br> <code>number_i+1 = (a * number_i + countingBits) mod
         * size</code><br> <br> The RNG is initialized using the value to be hashed.
         */
        RNG(HashProvider::hashRNG),
        /**
         * Generates hash values using the Carter Wegman function (<a href="http://en.wikipedia.org/wiki/Universal_hashing">Wikipedia</a>),
         * which is a universal hashing function. It thus has optimal guarantees for the uniformity of generated hash
         * values. On the downside, the performance is not optimal, as arithmetic operations on large numbers have to be
         * performed.
         */
        CarterWegman(HashProvider::hashCarterWegman),
        /**
         * Generates hash values using a Cyclic Redundancy Check (CRC32). CRC is designed as a checksum for data
         * integrity not as hash function but exhibits very good uniformity and is relatively fast.
         */
        CRC32(HashProvider::hashCRC),
        /**
         * Generates hash values using the Adler32 Checksum algorithm. Adler32 is comparable to CRC32 but is faster at
         * the cost of a less uniform distribution of hash values.
         */
        Adler32(HashProvider::hashAdler),
        /**
         * Generates hash values using the Murmur 2 hash, see: https://code.google.com/p/smhasher/wiki/MurmurHash2
         * <p>
         * Murmur 2 is very fast. However, there is a flaw that affects the uniformity of some input values (for
         * instance increasing integers as strings).
         */
        Murmur2(HashProvider::hashMurmur2),
        /**
         * Generates hash values using the Murmur 3 hash, see: https://code.google.com/p/smhasher/wiki/MurmurHash3
         * <p>
         * Its uniformity is comparable to that of cryptographic hash functions but considerably faster.
         */
        Murmur3(HashProvider::hashMurmur3),
        /**
         * Uses a the Murmur 3 hash in combination with a performance optimization described by Kirsch and Mitzenmacher,
         * see: https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf - hash values are generated through the scheme
         * h_i = (h1 + i*h2) mod m <p> Though this method is asymptotically optimal our experiements revealed that
         * real-world performance is not as good as pure Murmur 3 hashes or cryptographic hash functions, in particular
         * for random words.</p>
         */
        Murmur3KirschMitzenmacher(HashProvider::hashCassandra),
        /**
         * Uses the Fowler–Noll–Vo (FNV) hash function to generate a hash values. It is superior to the standard
         * implementation in {@link Arrays} and can be easily implemented in most languages. Hashing then uses the very
         * simple Linear Congruential Generator scheme and the Java initialization constants. This method is intended to
         * be employed if the bloom filter has to be used in a language which doesn't support any of the other hash
         * functions. This hash function can then easily be implemented.
         */
        FNVWithLCG(HashProvider::hashSimpleLCG),
        /**
         * Generates a hash value using MD2. MD2 is rather slow an not as evenely distributed as other cryptographic
         * hash functions
         */
        MD2((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "MD2")),
        /**
         * Generates a hash value using the cryptographic MD5 hash function. It is fast and has good guarantees for the
         * uniformity of generated hash values, as the hash functions are designed for cryptographic use.
         */
        MD5((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "MD5")),
        /**
         * Generates a hash value using the cryptographic SHA1 hash function. It is fast but uniformity of hash values
         * is better for the second generation of SHA (256,384,512).
         */
        SHA1((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "SHA-1")),
        /**
         * Generates a hash value using the cryptographic SHA-256 hash function. It is fast and has good guarantees for
         * the uniformity of generated hash values, as the hash functions are designed for cryptographic use.
         */
        SHA256((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "SHA-256")),
        /**
         * Generates a hash value using the cryptographic SHA-384 hash function. It is fast and has good guarantees for
         * the uniformity of generated hash values, as the hash functions are designed for cryptographic use.
         */
        SHA384((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "SHA-384")),
        /**
         * Generates a hash value using the cryptographic SHA-512 hash function. It is fast and has good guarantees for
         * the uniformity of generated hash values, as the hash functions are designed for cryptographic use.
         */
        SHA512((bytes, m, k) -> HashProvider.hashCrypt(bytes, m, k, "SHA-512"));

        private HashFunction hashFunction;

        private HashMethod(HashFunction hashFunction) {
            this.hashFunction = hashFunction;
        }

        public HashFunction getHashFunction() {
            return hashFunction;
        }
    }
}
