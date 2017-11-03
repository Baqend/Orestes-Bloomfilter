package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.redis.helper.RedisPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created on 04.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class ExpirationQueueRedis implements ExpirationQueue<String> {
    private static final Logger LOG = LoggerFactory.getLogger(ExpirationQueueRedis.class);
    private static final int HASH_LENGTH = 8;
    private static final String PATTERN_SUFFIX = String.join("", Collections.nCopies(HASH_LENGTH, "?"));
    /**
     * Maximum delay between jobs in seconds
     */
    private final long MAX_JOB_DELAY = TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    /**
     * Minimum delay between jobs in seconds
     */
    private final long MIN_JOB_DELAY = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

    private final RedisPool pool;
    private final String queueKey;
    private final Function<ExpirationQueueRedis, Boolean> expirationHandler;
    private final ScheduledExecutorService scheduler;
    private final Random random = new Random();
    private ScheduledFuture<?> job;

    /**
     *  Creates an ExpirationQueueRedis.
     *
     * @param pool A Redis pool instance to use
     * @param queueKey The queueKey used for the queue within Redis
     * @param expirationHandler A function that handles expiring items. Returns true if successful, false otherwise.
     */
    public ExpirationQueueRedis(RedisPool pool, String queueKey, Function<ExpirationQueueRedis, Boolean> expirationHandler) {
        this.pool = pool;
        this.queueKey = queueKey;
        this.expirationHandler = expirationHandler;
        this.scheduler = Executors.newScheduledThreadPool(1);
        clear();
        scheduleJob(true, MAX_JOB_DELAY, TimeUnit.NANOSECONDS);
    }

    /**
     * Removes the given elements from the queue.
     *
     * @param elements The elements to remove.
     * @param p The redis pipeline to use.
     */
    public void removeElements(Collection<String> elements, PipelineBase p) {
        p.zrem(queueKey, elements.toArray(new String[elements.size()]));
    }

    /**
     * Returns all expired elements from this queue with their unique identifier. To get the actual element keys use the normalize.
     *
     * @param jedis The Jedis runtime to use.
     * @return All expired elements from this queue with their unique identifier. To get the actual element keys use the normalize
     */
    public Set<String> getExpiredItems(Jedis jedis) {
        final long now = System.nanoTime();
        return jedis.zrangeByScore(queueKey, 0, now);
    }

    /**
     * Normalizes the queue element keys from their unique form to their actual key name.
     *
     * @param uniqueKeyElements The key to normalize
     * @return The normalized key
     */
    public String normalize(String uniqueKeyElements) {
        return uniqueKeyElements.substring(0, uniqueKeyElements.length() - HASH_LENGTH);
    }

    @Override
    public int size() {
        return pool.safelyReturn(p -> p.zcard(queueKey).intValue());
    }

    @Override
    public boolean add(ExpiringItem<String> item) {
        pool.safelyDo(p -> {
            String hash;
            do {
                hash = createRandomHash();
            } while (p.zadd(queueKey, item.getExpiration(), item.getItem() + hash) == 0);
        });
        return true;
    }

    @Override
    public Collection<ExpiringItem<String>> getNonExpired() {
        List<Tuple> result = new ArrayList<>();
        pool.safelyDo(p -> {
            String cursor = "0";
            do {
                final ScanResult<Tuple> scanResult = p.zscan(queueKey, cursor);
                result.addAll(scanResult.getResult());
                cursor = scanResult.getStringCursor();
            } while (!cursor.equals("0"));
        });

        return result.stream()
            .map(tuple -> {
                String elemString = tuple.getElement();
                String item = elemString
                    .substring(0, elemString.length() - ExpirationQueueRedis.HASH_LENGTH);
                return new ExpiringItem<>(item, (long) tuple.getScore());
            }).collect(toList());
    }

    @Override
    public void clear() {
        pool.safelyDo(p -> p.del(queueKey));
    }

    @Override
    public boolean contains(String item) {
        return pool.safelyReturn(
            p -> !p.zscan(queueKey, "0", new ScanParams().match(item + PATTERN_SUFFIX)).getResult().isEmpty());
    }

    @Override
    public boolean remove(String item) {
        throw new UnsupportedOperationException("Cannot remove from ExpirationQueueRedis");
    }

    @Override
    public Stream<ExpiringItem<String>> streamEntries() {
        Set<Tuple> allTuples = pool.safelyReturn(p -> p.zrangeWithScores(queueKey, 0, -1));
        return allTuples.stream()
            .map(tuple -> new ExpiringItem<>(tuple.getElement(), (long) tuple.getScore()));
    }

    /**
     * Destroys the expiration queue by deleting its contents and metadata.
     */
    public void remove() {
        job.cancel(true);
    }

    /**
     * Triggers the expiration handling before the given delay is expired.
     *
     * @param delay The delay when to trigger expiration
     * @param unit The time unit of the delay
     */
    synchronized public void triggerExpirationHandling(long delay, TimeUnit unit) {
        long delayInSec = TimeUnit.SECONDS.convert(delay, unit);
        long currentDelay = job.getDelay(TimeUnit.SECONDS);
        long epsilon = 1;
        if (currentDelay > delayInSec + epsilon) {
            scheduleJob(false, delay, unit);
        }
    }

    /**
     * Schedules a new job to work the queue.
     * @param aboutToEnd Whether the last job is about to end or needs to be canceled
     * @param delay When to schedule the new job
     * @param unit The time unit for the delay
     */
    synchronized private void scheduleJob(boolean aboutToEnd, long delay, TimeUnit unit) {
        ScheduledFuture<?> currentJob = this.job;
        if (!aboutToEnd && currentJob != null) {
            currentJob.cancel(false);
        }
        job = scheduler.schedule(this::expirationJob, delay, unit);
    }

    /**
     * The job that starts handling expiring item from the queue.
     */
    private void expirationJob() {
        long nextDelay = MIN_JOB_DELAY;
        try {
            boolean success = expirationHandler.apply(this);
            nextDelay = success? estimateNextDelay() : MIN_JOB_DELAY;
        } catch (Exception e) {
            LOG.error("Error executing expiration job.", e);
        } finally {
            scheduleJob(true, nextDelay, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Returns the delay when to next check the queue for expired items.
     *
     * @return The delay when to next check the queue for expired items.
     */
    private long estimateNextDelay() {
        return pool.safelyReturn(p -> {
            final long now = System.nanoTime();
            long max = now + MAX_JOB_DELAY;
            Set<Tuple> nextQueueItems = p.zrangeByScoreWithScores(queueKey, now, max, 0, 5);

            if (nextQueueItems.isEmpty()) {
                return MAX_JOB_DELAY;
            }

            return nextQueueItems.stream()
                .skip(random.nextInt(nextQueueItems.size()))
                .findFirst()
                .map(Tuple::getScore)
                .map(it -> Math.min(MAX_JOB_DELAY, it.longValue() - now))
                .orElse(MAX_JOB_DELAY);
        });
    }

    private String createRandomHash() {
        return UUID.randomUUID().toString().substring(0, HASH_LENGTH);
    }
}
