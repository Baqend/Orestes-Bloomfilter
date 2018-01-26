package orestes.bloomfilter.cachesketch;


import orestes.bloomfilter.TimeMap;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpirationQueueMemory<T> implements ExpirationQueue<T> {
    private volatile Future<?> future;
    private volatile boolean isEnabled;
    private final DelayQueue<ExpiringItem<T>> delayedQueue;
    private final Consumer<ExpiringItem<T>> handler;
    private final ExecutorService delayedQueueExecutorService = Executors.newSingleThreadExecutor();

    public ExpirationQueueMemory(Consumer<ExpiringItem<T>> handler) {
        this.delayedQueue = new DelayQueue<>();
        this.handler = handler;
        enable();
    }

    @Override
    public synchronized boolean enable() {
        if (isEnabled) return false;

        isEnabled = true;
        future = delayedQueueExecutorService.submit(() -> {
            try {
                while (isEnabled) {
                    // take() blocks until the next item expires
                    ExpiringItem<T> e = delayedQueue.take();
                    if (e.getItem() != null) {
                        this.handler.accept(e);
                    }
                }
            } catch (InterruptedException ignored) {
            }
        });
        return true;
    }

    @Override
    public synchronized boolean disable() {
        if (!isEnabled) return false;

        isEnabled = false;
        delayedQueue.add(new ExpiringItem<>(null, 0, NANOSECONDS));
        try {
            future.get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
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
        Optional<ExpiringItem<T>> found = delayedQueue.stream().filter(it -> it.getItem().equals(item)).findFirst();
        return found.filter(delayedQueue::remove).isPresent();
    }

    @Override
    public TimeMap<T> getExpirationMap() {
        return delayedQueue.stream()
            // filter out our fake item
            .filter(item -> item.getItem() != null)
            .collect(
                TimeMap::new,
                (timeMap, item) -> timeMap.put(item.getItem(), item.getExpiration(MILLISECONDS)),
                TimeMap::putAll
            );
    }

    @Override
    public void setExpirationMap(TimeMap<T> map) {
        map.forEach((item, ttl) -> addExpiration(item, ttl, MILLISECONDS));
    }
}
