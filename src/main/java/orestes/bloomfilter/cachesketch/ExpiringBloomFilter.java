package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Tracks a mapping from objects to expirations and a Bloom filter of objects that are automatically removed after
 * expiration(obj).
 *
 */
public interface ExpiringBloomFilter<T> extends CountingBloomFilter<T> {

    /**
     * Determines whether a given object is no-expired
     *
     * @param element the element (or its id)
     * @return <code>true</code> if the element is non-expired
     */
    boolean isCached(T element);

    /**
     * Return the expiration timestamp of an object
     *
     * @param element the element (or its id)
     * @param unit    the time unit of the returned ttl
     * @return the remaining ttl
     */
    Long getRemainingTTL(T element, TimeUnit unit);

    /**
     * Return the expiration timestamps of the given object
     *
     * @param elements elements to check
     * @param unit    the time unit of the returned ttl
     * @return the remaining ttl
     */
    default List<Long> getRemainingTTLs(List<T> elements, TimeUnit unit){
        return elements.stream().map(el -> getRemainingTTL(el, unit)).collect(Collectors.toList());
    }

    /**
     * Reports a read on element that is to be cached for a certain ttl
     *
     * @param element the element (or its id)
     * @param TTL     the TTL which the element is cached
     * @param unit    the time unit of the provided ttl
     */
    void reportRead(T element, long TTL, TimeUnit unit);

    /**
     * Reports a write on an object, adding it to the underlying Bloom filter for the remaining ttl
     *
     * @param element the element (or its id)
     * @param unit the time unit of the returned ttl
     * @return the remaining TTL, if the object was still cached, else <code>null</code>
     */
    Long reportWrite(T element, TimeUnit unit);

    /**
     * Reports a write.
     *
     * @param element the element (or its id)
     * @return <code>true</code>, if the elements needs invalidation
     */
    default boolean reportWrite(T element) {
        return reportWrite(element, TimeUnit.MILLISECONDS) != null;
    }

    BloomFilter<T> getClonedBloomFilter();
}
