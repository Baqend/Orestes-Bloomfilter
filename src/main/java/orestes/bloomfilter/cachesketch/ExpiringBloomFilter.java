package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class ExpiringBloomFilter<T> extends CountingBloomFilterMemory<T> {
    private final DelayQueue<ExpiringItem<T>> delayedQueue = new DelayQueue<>();
    private final Map<T, Long> expirations = new ConcurrentHashMap<>();
    private volatile Thread workerThread;

    private final Runnable queueWorker = () -> {
        try {
            while (true) {
                ExpiringItem<T> e = delayedQueue.take();
                removeAndEstimateCount(e.getItem());
            }
        } catch (InterruptedException e) {
        }
    };

    public ExpiringBloomFilter(FilterBuilder config) {
        super(config);
        workerThread = new Thread(queueWorker);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    private Long ttlToTimestamp(long TTL) {
        return System.nanoTime() + TTL;
    }

    public boolean isCached(T element) {
        Long ts = expirations.get(element);
        return ts != null && ts > System.nanoTime();
    }

    public synchronized void reportRead(T element, long TTL) {
        expirations.computeIfAbsent(element, (k) -> ttlToTimestamp(TTL));
        expirations.computeIfPresent(element, (k, v) -> Math.max(ttlToTimestamp(TTL), v));
    }

    public synchronized void reportWrite(T element) {
        //Only add if there is a potentially cached read
        if(isCached(element)) {
            add(element);
            delayedQueue.add(new ExpiringItem<T>(element, expirations.get(element)));
        }
    }

    public static class ExpiringItem<T> implements Delayed {
        private final T item;
        private final long expires;


        public ExpiringItem(T item, long expires) {
            this.item = item;
            this.expires = expires;
        }


        public T getItem() {
            return item;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expires - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed delayed) {
            return Long.compare(expires, delayed.getDelay(TimeUnit.NANOSECONDS));
        }

        @Override
        public String toString() {
            return getItem() + " expires in " + getDelay(TimeUnit.SECONDS) + "s";
        }
    }

}
