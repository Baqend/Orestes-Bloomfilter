package orestes.bloomfilter;

/**
 * A Bloom filter that can be migrated to from another implementation.
 *
 * @author Erik Witt
 * @author Konstantin Simon Maria Möllers
 */
public interface MigratableBloomFilter<U, T extends BloomFilter<U>> {

    /**
     * Migrates to this Bloom filter from a given source.
     *
     * @param source The Bloom filter source.
     * @return The migrated Bloom filter.
     */
    T migrateFrom(BloomFilter<U> source);

    static class IncompatibleMigrationSourceException extends RuntimeException {

        public IncompatibleMigrationSourceException(String msg) {
            super(msg);
        }

        public IncompatibleMigrationSourceException(String msg, Throwable t) {
            super(msg, t);
        }
    }
}
