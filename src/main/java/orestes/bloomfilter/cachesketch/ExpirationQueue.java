package orestes.bloomfilter.cachesketch;


import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExpirationQueue<T> implements Iterable<T> {
    private final Thread workerThread;
    private final DelayQueue<ExpiringItem<T>> delayedQueue;
    private final Consumer<ExpiringItem<T>> handler;

    public ExpirationQueue(Consumer<ExpiringItem<T>> handler) {
        this.delayedQueue =  new DelayQueue<>();
        this.handler = handler;
        this.workerThread = new Thread(() -> {
            try {
                while (true) {
                    // take() blocks until the next item expires
                    ExpiringItem<T> e = delayedQueue.take();
                    this.handler.accept(e);
                }
            } catch (InterruptedException ignored) {
            }
        });
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Adds an item with a time to live (TTL) to the queue.
     *
     * @param item The item to add to the queue
     * @param ttl The time to live of the item, in nanoseconds
     * @return whether the item has been added
     */
    public boolean addTTL(T item, long ttl) {
        return addTTL(item, ttl, TimeUnit.NANOSECONDS);
    }

    /**
     * Adds an item with a time to live (TTL) to the queue.
     *
     * @param item The item to add to the queue
     * @param ttl The time to live of the item
     * @param ttlUnit The unit of the ttl
     * @return whether the item has been added
     */
    public boolean addTTL(T item, long ttl, TimeUnit ttlUnit) {
        return add(new ExpiringItem<>(item, System.nanoTime() + TimeUnit.NANOSECONDS.convert(ttl, ttlUnit)));
    }

    /**
     * Adds an item with an expiration timestamp to the queue.
     *
     * @param item The item to add to the queue
     * @param timestamp The timestamp when the item expires, in nanoseconds since {@link System#nanoTime()}
     * @return whether the item has been added
     */
    public boolean addExpiration(T item, long timestamp) {
        return add(new ExpiringItem<>(item, timestamp));
    }

    /**
     * Returns the queue's size.
     *
     * @return the size of this queue
     */
    public int size() {
        return delayedQueue.size();
    }

    /**
     * Adds an expiring item to the queue.
     *
     * @param item The item to add to the queue
     * @return whether the item has been added
     */
    public boolean add(ExpiringItem<T> item) {
        return delayedQueue.add(item);
    }

    /**
     * Returns the items in the queue which are not expired yet.
     *
     * @return a queue of non-expired items
     */
    public Queue<ExpiringItem<T>> getNonExpired() {
        return delayedQueue;
    }

    /**
     * Clears the queue.
     */
    public void clear() {
        delayedQueue.clear();
    }

    /**
     * Checks whether this queue contains a given item.
     *
     * @param item The item to check
     * @return true, if item is contained
     */
    public boolean contains(T item) {
        return delayedQueue.stream().anyMatch(it -> it.item.equals(item));
    }

    /**
     * Removes the given item from this queue.
     *
     * @param item The item to remove
     * @return true, if item has been removed
     */
    public boolean remove(T item) {
        final Optional<ExpiringItem<T>> found = delayedQueue.stream().filter(it -> it.item.equals(item)).findFirst();
        return found.filter(delayedQueue::remove).isPresent();
    }

    @Override
    public Iterator<T> iterator() {
        return delayedQueue.stream().map(it -> it.item).iterator();
    }

    public static class ExpiringItem<T> implements Delayed {
        private final T item;
        private final long expiration;

        /**
         * Creates an ExpiringItem.
         *
         * @param item The actual item which expires
         * @param expiration The expiration timestamp, relative to {@link java.lang.System#nanoTime()}
         */
        public ExpiringItem(T item, long expiration) {
            this.item = item;
            this.expiration = expiration;
        }

        /**
         * @return the actual item which expires
         */
        public T getItem() {
            return item;
        }

        /**
         * @return the timestamp when the item expires
         */
        public long getExpiration() {
            return expiration;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expiration - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public String toString() {
            return getItem() + " expires in " + getDelay(TimeUnit.SECONDS) + "s";
        }
    }
}
