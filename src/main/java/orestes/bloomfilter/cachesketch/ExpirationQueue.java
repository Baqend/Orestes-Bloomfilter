package orestes.bloomfilter.cachesketch;


import java.util.Collection;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExpirationQueue<T> {
    private final Thread workerThread;
    private DelayQueue<ExpiringItem<T>> delayedQueue;
    private Consumer<ExpiringItem<T>> handler;

    private final Runnable queueWorker = () -> {
        try {
            while (true) {
                ExpiringItem<T> e = delayedQueue.take();
                handler.accept(e);
            }
        } catch (InterruptedException e) {
        }
    };

    public ExpirationQueue(Consumer<ExpiringItem<T>> handler) {
        this.delayedQueue =  new DelayQueue<>();
        this.handler = handler;
        this.workerThread = new Thread(queueWorker);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void addTTL(T item, long ttl) {
        add(new ExpiringItem<>(item, System.nanoTime() + ttl));
    }

    public void addExpiration(T item, long timestamp) {
        add(new ExpiringItem<>(item, timestamp));
    }

    public int size() {
        return delayedQueue.size();
    }

    public void add(ExpiringItem<T> item) {
        delayedQueue.add(item);
    }

    public Collection<ExpiringItem<T>> getNonExpired() {
        return delayedQueue;
    }

    public void clear() {
        delayedQueue.clear();
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

        public long getExpiration() {
            return expires;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expires - System.nanoTime(), TimeUnit.NANOSECONDS);
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
