package orestes.bloomfilter.memory;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.MigratableBloomFilter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;


public class CountingBloomFilterMemory<T> implements CountingBloomFilter<T>, MigratableBloomFilter<T> {
    private static final long serialVersionUID = -3207752201903871264L;
    protected FilterBuilder config;
    protected BloomFilterMemory<T> filter;
    protected BitSet counts;
    protected transient Runnable overflowHandler = () -> { };

    protected CountingBloomFilterMemory() { }


    public CountingBloomFilterMemory(FilterBuilder config) {
        config.complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counts = new BitSet(config.size() * config().countingBits());
    }

    @Override
    public boolean contains(byte[] element) {
        return filter.contains(element);
    }


    @Override
    public Map<Integer, Long> getCountMap() {
        Map<Integer, Long> result = new HashMap<>();
        for (int i = 0, low = 0, high = low + config().countingBits(); low < counts.length(); i++, low += config.countingBits(), high += config.countingBits()) {
            long count = 0;
            for (int j = low; j < high; j++) {
                count <<= 1;
                if (counts.get(j)) {
                    count |= 1;
                }
            }

            if (count > 0) {
                result.put(i, count);
            }
        }

        return result;
    }

    @Override
    public synchronized long addAndEstimateCountRaw(byte[] element) {
        // Calculate the hashes of this element
        return IntStream.of(hash(element))
            .mapToLong(hash -> {
                // Set each bit at the position "hash"
                filter.setBit(hash, true);

                // Increment the count at the position "hash" and return the new value
                return increment(hash);
            })
            // Get the estimated count for the element by finding the minimal value
            .min().getAsLong();
    }


    @Override
    public synchronized long removeAndEstimateCountRaw(byte[] element) {
        if (!contains(element)) { return 0; }

        // Calculate the hashes of this element
        return IntStream.of(hash(element))
            .mapToLong(hash -> {
                // Decrement the count at the position "hash" and return the new value
                long count = decrement(hash);

                // Remove each bit at the position "hash" if count is now zero
                filter.setBit(hash, count > 0);

                return count;
            })
            // Get the estimated count for the element by finding the minimal value
            .min().getAsLong();
    }


    /**
     * Increment the internal counter upon insertion of new elements.
     *
     * @param index position at which to increase
     * @return the new counter value
     */
    protected long increment(int index) {
        int low = index * config().countingBits();
        int high = (index + 1) * config().countingBits();

        // Do a binary +1 on the slice of length countingBits
        boolean incremented = false;
        long count = 0;
        int pos = 0;
        for (int i = (high - 1); i >= low; i--) {
            if (!counts.get(i) && !incremented) {
                counts.set(i);
                incremented = true;
            } else if (!incremented) {
                counts.set(i, false);
            }

            if (counts.get(i)) {
                count += Math.pow(2, pos);
            }
            pos++;
        }

        // If the counter overflowed, call the handler
        // and set the counter to the maximum value
        if (!incremented) {
            overflowHandler.run();
            for (int i = (high - 1); i >= low; i--) {
                counts.set(i);
            }
            //return max value
            count = (long) Math.pow(2, config().countingBits() - 1);
        }
        return count;
    }

    protected long count(int index) {
        int low = index * config().countingBits();
        int high = low + config().countingBits();

        long count = 0;
        //bit * 2^0 + bit * 2^1 ...
        for (int i = low; i < high; i++) {
            count <<= 1;
            if (counts.get(i)) {
                count |= 1;
            }
        }
        return count;
    }

    protected void set(int index, long newValue) {
        int low = index * config().countingBits();
        int high = low + config().countingBits() - 1;

        //bit * 2^0 + bit * 2^1 ...
        for (int i = high; i >= low; i--) {
            counts.set(i, (newValue & 1) > 0);
            newValue >>>= 1;
        }
    }

    /**
     * Decrements the internal counter upon deletion and unsets the Bloom filter bit if necessary.
     *
     * @param index position at which to decrease
     * @return the new counter value
     */
    protected long decrement(int index) {
        int low = index * config().countingBits();
        int high = (index + 1) * config().countingBits();

        // Do a binary -1 on the counter's slice of length countingBits
        boolean decremented = false;
        boolean nonZero = false;
        long count = 0;
        int pos = 0;
        for (int i = (high - 1); i >= low; i--) {
            if (!decremented) {
                // Flip every bit until you reach the first bit that is one. Flip that and stop flipping.
                if (counts.get(i)) {
                    counts.set(i, false);
                    decremented = true;
                } else {
                    counts.set(i, true);
                    nonZero = true;
                }
            } else {
                // While there is still one bit that is not zero the counter isn't zero
                if (counts.get(i)) {
                    nonZero = true;
                }
            }
            if (counts.get(i)) {
                count += Math.pow(2, pos);
            }
            pos++;
        }

        return count;
    }

    @Override
    public synchronized long getEstimatedCount(T element) {
        return IntStream.of(hash(toBytes(element))).mapToLong(this::count).min().getAsLong();
    }

    @Override
    public boolean union(BloomFilter<T> other) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean intersect(BloomFilter<T> other) {
        //TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return filter.isEmpty();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder(asString() + "\n");
        for (int i = 0; i < config().size(); i++) {
            sb.append(filter.getBit(i) ? 1 : 0);
            sb.append(" ");
            if (counts != null) {
                for (int j = 0; j < config().countingBits(); j++) {
                    sb.append(counts.get(config().countingBits() * i + j) ? 1 : 0);
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized CountingBloomFilterMemory<T> clone() {
        CountingBloomFilterMemory<T> o = null;
        try {
            o = (CountingBloomFilterMemory<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        o.filter = (BloomFilterMemory<T>) this.filter.clone();
        if (this.counts != null) { o.counts = (BitSet) this.counts.clone(); }
        o.config = this.config.clone();
        return o;
    }

    @Override
    public void clear() {
        filter.clear();
        counts.clear();
    }


    @Override
    public BitSet getBitSet() {
        return filter.getBitSet();
    }

    @Override
    public FilterBuilder config() {
        return this.config;
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof CountingBloomFilterMemory)) { return false; }

        CountingBloomFilterMemory that = (CountingBloomFilterMemory) o;

        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) { return false; }
        if (counts != null ? !counts.equals(that.counts) : that.counts != null) { return false; }
        if (filter != null ? !filter.equals(that.filter) : that.filter != null) { return false; }

        return true;
    }


    public void setOverflowHandler(Runnable callback) {
        this.overflowHandler = callback;
    }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        this.overflowHandler = () -> {};
    }

    public BloomFilterMemory<T> getBloomFilter() {
        return filter;
    }

    @Override
    public void migrateFrom(BloomFilter<T> source) {
        if (!(source instanceof CountingBloomFilter) || !compatible(source)) {
            throw new IncompatibleMigrationSourceException("Source is not compatible with the targeted Bloom filter");
        }

        CountingBloomFilter<T> cbf = (CountingBloomFilter<T>) source;
        cbf.getCountMap().forEach((position, value) -> {
            set(position, value);
            filter.setBit(position, value > 0);
        });
    }
}
