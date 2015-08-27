package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.CountingBloomFilter;

import java.util.concurrent.TimeUnit;

public interface ExpiringBloomFilter<T> extends CountingBloomFilter<T> {

    /**
     * Determines whether a given object is no-expired
     *
     * @param element the element (or its id)
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
     * Reports a read on element that is to be cached for a certain ttl
     *
     * @param element the element (or its id)
     * @param TTL     the TTL which the element is cached
     * @param unit    the time unit of the provided ttl
     */
    void reportRead(T element, long TTL, TimeUnit unit);

    /**
     * Reports a write on objects, adding it to the underlying Bloom filter for the remaining ttl
     *
     * @param element the element (or its id)
     * @param unit the time unit of the returned ttl
     * @return the remaining TTL, if the object was still cached, else <code>null</code>
     */
    Long reportWrite(T element, TimeUnit unit);

    default void reportWrite(T element) {
        reportWrite(element, TimeUnit.MILLISECONDS);
    }
}
