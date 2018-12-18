package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.ExpirationMapAware;

import java.time.Clock;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created on 04.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public interface ExpirationQueue<T> extends Iterable<T>, ExpirationMapAware<T> {
    /**
     * Adds an item with a time to live (TTL) to the queue.
     *
     * @param item    The item to add to the queue
     * @param ttl     The time to live of the item
     * @param ttlUnit The unit of the ttl
     * @return whether the item has been added
     */
    default boolean addTTL(T item, long ttl, TimeUnit ttlUnit) {
        return add(new ExpiringItem<>(item, now() + MILLISECONDS.convert(ttl, ttlUnit), MILLISECONDS));
    }

    /**
     * Adds an item with an expiration timestamp to the queue.
     *
     * @param item      The item to add to the queue
     * @param timestamp The timestamp when the item expires, in nanoseconds since {@link System#nanoTime()}
     * @param timeUnit  The timestamp's time unit.
     * @return whether the item has been added
     */
    default boolean addExpiration(T item, long timestamp, TimeUnit timeUnit) {
        return add(new ExpiringItem<>(item, timestamp, timeUnit));
    }

    /**
     * Enables the expiration queue, continuing it to expire items.
     *
     * @return {@code true}, if queue could be enabled.
     */
    boolean enable();

    /**
     * Disables the expiration queue, stopping it from expire items.
     *
     * @return {@code true}, if queue could be disabled.
     */
    boolean disable();

    /**
     * Either enables or disables the queue, controllig if it expires items.
     *
     * @param enabled If {@code true}, the queue should be expiring items.
     * @return {@code true}, if queue could be enabled or disabled.
     */
    default boolean setEnabled(boolean enabled) {
        return enabled ? enable() : disable();
    }

    /**
     * Returns the queue's size.
     *
     * @return the size of this queue
     */
    int size();

    /**
     * Adds an expiring item to the queue.
     *
     * @param item The item to add to the queue.
     * @return whether the item has been added.
     */
    boolean add(ExpiringItem<T> item);

    /**
     * Returns the items in the queue which are not expired yet.
     *
     * @return a queue of non-expired items
     */
    Collection<ExpiringItem<T>> getNonExpired();

    /**
     * Clears the queue.
     */
    void clear();

    /**
     * Checks whether this queue contains a given item.
     *
     * @param item The item to check
     * @return true, if item is contained
     */
    boolean contains(T item);

    /**
     * Removes the given item from this queue.
     *
     * @param item The item to remove
     * @return true, if item has been removed
     */
    boolean remove(T item);

    /**
     * Returns the current clock in milliseconds.
     *
     * @return The current point in time.
     */
    default long now() {
        return Clock.systemUTC().millis();
    }

    @Override
    default Iterator<T> iterator() {
        return getNonExpired().stream().map(ExpiringItem::getItem).iterator();
    }

    /**
     * An item of the ExpirationQueue
     *
     * @param <T> The type of the item held by the queue
     */
    class ExpiringItem<T> implements Delayed {
        private final T item;
        private final long expiration;

        /**
         * Creates an ExpiringItem.
         *
         * @param item       The actual item which expires
         * @param expiration The expiration timestamp, relative to {@link java.lang.System#nanoTime()}
         * @param timeUnit   The {@link TimeUnit} of the given expiration
         */
        public ExpiringItem(T item, long expiration, TimeUnit timeUnit) {
            this.item = item;
            this.expiration = MILLISECONDS.convert(expiration, timeUnit);
        }

        /**
         * @return the actual item which expires
         */
        public T getItem() {
            return item;
        }

        /**
         * @param timeUnit The {@link TimeUnit} to which the expiration is converted
         * @return the timestamp in nanoseconds when the item expires
         */
        public long getExpiration(TimeUnit timeUnit) {
            return timeUnit.convert(expiration, MILLISECONDS);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expiration - Clock.systemUTC().millis(), MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Long.compare(getDelay(MILLISECONDS), delayed.getDelay(MILLISECONDS));
        }

        @Override
        public String toString() {
            return getItem() + " expires in " + getDelay(TimeUnit.SECONDS) + "s";
        }
    }
}
