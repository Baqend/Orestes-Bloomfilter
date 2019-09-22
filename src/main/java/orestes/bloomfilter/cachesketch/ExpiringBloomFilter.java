package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Tracks a mapping from objects to expirations and a Bloom filter of objects that are automatically removed after
 * expiration(obj).
 *
 */
public interface ExpiringBloomFilter<T> extends CountingBloomFilter<T>, TimeToLiveMapAware<T>, ExpirationMapAware<T>, MigratableBloomFilter<T> {

    /**
     * Determines whether a given object is no-expired
     *
     * @param element the element (or its id)
     * @return <code>true</code> if the element is non-expired
     */
    default boolean isCached(T element) {
        return getRemainingTTL(element, TimeUnit.MILLISECONDS) != null;
    }

    /**
     * Determines whether a given object is known
     *
     * An object is known if it is cached or was removed from the cache not for
     * longer than the grace period.
     *
     * @param element the element (or its id)
     * @return <code>true</code> if the element is known; <code>false</code> else
     */
    boolean isKnown(T element);

    /**
     * Determines whether each of the given input elements is known and returns a list of boolean values where each
     * Boolean represents whether or not the corresponding element in the input list is known.
     *
     * An object is known if it is cached or was removed from the cache not for longer than the grace period.
     *
     * @param elements elements to check (or their ids)
     * @return a list of boolean values indicating whether each of the input elements is known (<code>true</code>) or
     * not (<code>false</code>)
     */
    List<Boolean> isKnown(List<T> elements);

    /**
     * Return the expiration timestamp of an object
     *
     * @param element the element (or its id)
     * @param unit    the time unit of the returned ttl
     * @return the remaining ttl
     */
    default Long getRemainingTTL(T element, TimeUnit unit) {
        return getTimeToLiveMap().getRemaining(element, unit);
    }

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
     * Cleans all expired time to live entries that have been tracked.
     */
    default void cleanupTTLs() {
        throw new UnsupportedOperationException();
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

    /**
     * Reports multiple writes.
     *
     * @param elements the elements (or its ids)
     * @return List of booleans. <code>true</code>, if the elements needs invalidation
     */
    default List<Boolean> reportWrites(List<T> elements) {
        return elements.stream().map(this::reportWrite).collect(Collectors.toList());
    }

    /**
     * Reports a write on a list of objects, adding it to the underlying Bloom filter for the remaining ttl
     *
     * @param elements the elements (or its ids)
     * @param unit the time unit of the returned ttl
     * @return the list of remaining TTLs, if a object was still cached, else <code>null</code>
     */
    default List<Long> reportWrites(List<T> elements, TimeUnit unit) {
        return elements.stream().map(el -> reportWrite(el, unit)).collect(Collectors.toList());
    }

    /**
     * Sets whether expiration should be turned on.
     *
     * @param enabled The boolean to enable or disable the expiration
     * @return Whether state could be changed successfully.
     */
    boolean setExpirationEnabled(boolean enabled);

    default boolean enableExpiration() {
        return setExpirationEnabled(true);
    }

    default boolean disableExpiration() {
        return setExpirationEnabled(false);
    }

    BloomFilter<T> getClonedBloomFilter();

    /**
     * Executes a soft clear operation on the expiring Bloom filter.
     * Clears the bloom filter without deleting any TTLs
     */
    void softClear();
}
