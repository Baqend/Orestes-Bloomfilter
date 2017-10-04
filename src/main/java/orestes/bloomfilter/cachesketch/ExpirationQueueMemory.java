package orestes.bloomfilter.cachesketch;


import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExpirationQueueMemory<T> implements ExpirationQueue<T> {
    private final Thread workerThread;
    private final DelayQueue<ExpiringItem<T>> delayedQueue;
    private final Consumer<ExpiringItem<T>> handler;

    public ExpirationQueueMemory(Consumer<ExpiringItem<T>> handler) {
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

    @Override
    public int size() {
        return delayedQueue.size();
    }

    @Override
    public boolean add(ExpiringItem<T> item) {
        return delayedQueue.add(item);
    }

    @Override
    public Queue<ExpiringItem<T>> getNonExpired() {
        return delayedQueue;
    }

    @Override
    public void clear() {
        delayedQueue.clear();
    }

    @Override
    public boolean contains(T item) {
        return delayedQueue.stream().anyMatch(it -> it.getItem().equals(item));
    }

    @Override
    public boolean remove(T item) {
        final Optional<ExpiringItem<T>> found = delayedQueue.stream().filter(it -> it.getItem().equals(item)).findFirst();
        return found.filter(delayedQueue::remove).isPresent();
    }
}
