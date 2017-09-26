package orestes.bloomfilter;

/**
 * A Bloom filter that can be migrated to from another implementation.
 *
 * @author Erik Witt
 * @author Konstantin Simon Maria Möllers
 */
public interface MigratableBloomFilter<T extends BloomFilter> {

    /**
     * Migrates to this Bloom filter from a given source.
     *
     * @param source The Bloom filter source.
     * @param <U> The type of the Bloom filter source.
     * @return The migrated Bloom filter.
     */
    <U> T migrateFrom(BloomFilter<U> source);
}
