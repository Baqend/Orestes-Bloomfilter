package orestes.bloomfilter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a Counting Bloom Filter, which in contrast to a normal Bloom filter also allow removal.
 */
public interface CountingBloomFilter<T> extends BloomFilter<T> {

    /**
     * @return the number of bits used for counting
     */
    public default int getCountingBits() {
        return config().countingBits();
    }

    /**
     * Returns the counts contained by this Bloom filter as a map.
     *
     * @return a map of positions to Bloom filter counts
     */
    public Map<Integer, Long> getCountMap();


    @Override
    public default boolean addRaw(byte[] element) {
        return addAndEstimateCountRaw(element) == 1;
    }

    /**
     * Removes the object from the counting bloom filter.
     *
     * @param element object to be deleted
     * @return {@code true} if the element is not present after removal
     */
    public default boolean removeRaw(byte[] element) {
        return removeAndEstimateCountRaw(element) <= 0;
    }

    /**
     * Removes the object from the counting bloom filter.
     *
     * @param element object to be deleted
     * @return {@code true} if the element is not present after removal
     */
    public default boolean remove(T element) {
        return removeRaw(toBytes(element));
    }


    /**
     * Removes the objects from the counting bloom filter.
     *
     * @param elements objects to be deleted
     * @return a list of booleans indicating for each element, whether it was removed
     */
    public default List<Boolean> removeAll(Collection<T> elements) {
        return elements.stream().map(this::remove).collect(Collectors.toList());
    }

    /**
     * Return the estimated count for an element using the Mininum Selection algorithm (i.e. by choosing the minimum
     * counter for the given element). This estimation is biased, as it doest not consider how full the filter is, but
     * performs best in practice. The underlying theoretical foundation are spectral Bloom filters, see:
     * http://theory.stanford.edu/~matias/papers/sbf_thesis.pdf
     *
     * @param element element to query
     * @return estimated count of the element
     */
    public long getEstimatedCount(T element);

    /**
     * Adds an element and returns its estimated frequency after the insertion (i.e. the number of times the element was
     * added to the filter).
     *
     * @param element element to add
     * @return estimated frequency of the element after insertion
     */
    public long addAndEstimateCountRaw(byte[] element);

    /**
     * Adds an element and returns its estimated frequency after the insertion (i.e. the number of times the element was
     * added to the filter).
     *
     * @param element element to add
     * @return estimated frequency of the element after insertion
     */
    public default long addAndEstimateCount(T element) {
        return addAndEstimateCountRaw(toBytes(element));
    }

    /**
     * Removes an element and returns its estimated frequency after the insertion (i.e. the number of times the element
     * was added to the filter).
     *
     * @param element element to remove
     * @return estimated frequency of the element after deletion
     */
    public long removeAndEstimateCountRaw(byte[] element);

    /**
     * Removes an element and returns its estimated frequency after the insertion (i.e. the number of times the element
     * was added to the filter).
     *
     * @param element element to remove
     * @return estimated frequency of the element after deletion
     */
    public default long removeAndEstimateCount(T element) {
        return removeAndEstimateCountRaw(toBytes(element));
    }

    /**
     * @return copy of the filter.
     */
    public CountingBloomFilter<T> clone();

}
