package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.redis.MessagePackEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by erik on 09.10.17.
 */
public class ExpiringBloomFilterPureRedis extends AbstractExpiringBloomFilterRedis<String> {
    /**
     * Maximum delay between jobs in milliseconds.
     */
    private static final long MAX_JOB_DELAY = MILLISECONDS.convert(60, TimeUnit.SECONDS);

    /**
     * Minimum delay between jobs in milliseconds.
     */
    private static final long MIN_JOB_DELAY = 100;

    /**
     * Logger for the {@link ExpiringBloomFilterPureRedis} class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExpiringBloomFilterPureRedis.class);

    private final String expireQueueScript = loadLuaScript("expireQueue.lua");
    private final Random random = new Random();
    private final MessagePackEncoder msgPack;
    private ScheduledFuture<?> job;
    private boolean isEnabled;

    public ExpiringBloomFilterPureRedis(FilterBuilder builder) {
        super(builder);
        this.msgPack = new MessagePackEncoder();
        enableJob();
    }

    @Override
    public void clear() {
        try (Jedis jedis = pool.getResource()) {
            // Delete all used fields from Redis
            jedis.del(keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY, keys.TTL_KEY);
        }
    }

    @Override
    public void softClear() {
        try (Jedis jedis = pool.getResource()) {
            // Delete all used fields from Redis
            jedis.del(keys.COUNTS_KEY, keys.BITS_KEY, keys.EXPIRATION_QUEUE_KEY);
        }
    }

    @Override
    public synchronized void remove() {
        super.remove();
        if (job != null) {
            job.cancel(true);
            job = null;
        }
    }

    @Override
    public void addToQueue(String item, long remaining, TimeUnit timeUnit) {
        try (Jedis jedis = pool.getResource()) {
            boolean done;
            do {
                int[] positions = hash(item.getBytes());
                byte[] member = msgPack.encodeItem(item, positions);
                done = jedis.zadd(keys.EXPIRATION_QUEUE_KEY.getBytes(), now() + MILLISECONDS.convert(remaining, timeUnit), member) == 1;
            } while (!done);
        }
        triggerExpirationHandling(remaining, timeUnit);
    }

    /**
     * Handles expiring items from the expiration queue.
     *
     * @return true if successful, false otherwise.
     */
    public synchronized boolean onExpire() {
        long now = now();
        LOG.debug("[{}] Expiring items ... {}", config.name(), now);
        try (Jedis jedis = pool.getResource()) {
            long expiredItems = (long) jedis.evalsha(
                expireQueueScript, 3,
                // Keys:
                keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY,
                // Args:
                String.valueOf(now)
            );
            LOG.debug("[{}] Script expired {} items within {}ms", config.name(), expiredItems, now() - now);
            return true;
        }
    }


    @Override
    public TimeMap<String> getExpirationMap() {
        try (Jedis jedis = pool.getResource()) {
            return jedis.zrangeWithScores(keys.EXPIRATION_QUEUE_KEY.getBytes(), 0, -1)
                .stream()
                .collect(TimeMap.collectMillis(
                    tuple -> msgPack.decodeItem(tuple.getBinaryElement()),
                    tuple -> (long) tuple.getScore()
                ));
        }
    }

    @Override
    public void setExpirationMap(TimeMap<String> map) {
        try (Jedis jedis = pool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            AtomicInteger ctr = new AtomicInteger(0);
            map.forEach((item, expiration) -> {
                int[] positions = hash(item);
                pipeline.zadd(keys.EXPIRATION_QUEUE_KEY.getBytes(), expiration, msgPack.encodeItem(item, positions));

                if (ctr.incrementAndGet() >= 1000) {
                    ctr.set(0);
                    pipeline.sync();
                }
            });
            pipeline.sync();
        }
    }

    @Override
    public boolean setExpirationEnabled(boolean enabled) {
        return enabled ? enableJob() : disableJob();
    }

    /**
     * Triggers the expiration handling before the given delay is expired.
     *
     * @param delay The delay when to trigger expiration
     * @param unit The time unit of the delay
     */
    private synchronized void triggerExpirationHandling(long delay, TimeUnit unit) {
        if (!isEnabled) return;
        long delayInMilliseconds = MILLISECONDS.convert(delay, unit);
        long currentDelay = job.getDelay(MILLISECONDS);
        if (currentDelay > (delayInMilliseconds + MIN_JOB_DELAY)) {
            scheduleJob(false, delay, unit);
        }
    }

    /**
     * Schedules a new job to work the queue.
     * @param shouldNotCancel Whether the last job is about to end or needs to be canceled
     * @param delay When to schedule the new job
     * @param unit The time unit for the delay
     */
    private synchronized void scheduleJob(boolean shouldNotCancel, long delay, TimeUnit unit) {
        ScheduledFuture<?> currentJob = job;
        if (!shouldNotCancel && (currentJob != null)) {
            LOG.debug("[{}] Cancel active job", config.name());
            currentJob.cancel(false);
        }
        LOG.debug("[" + this.config.name() + "] Scheduled the next expiration job in " + MILLISECONDS.convert(delay, unit) + "ms");
        job = scheduler.schedule(this::expirationJob, delay, unit);
    }

    /**
     * The job that starts handling expiring item from the queue.
     */
    private void expirationJob() {
        long nextDelay = MIN_JOB_DELAY;
        try {
            boolean success = onExpire();
            nextDelay = success? estimateNextDelay() : MIN_JOB_DELAY;
        } catch (Exception e) {
            LOG.error("[" + this.config.name() + "] Error in script", e);
        } finally {
            scheduleJob(true, nextDelay, MILLISECONDS);
        }
    }

    /**
     * Returns the delay in milliseconds when to next check the queue for expired items.
     *
     * @return The delay in milliseconds.
     */
    private long estimateNextDelay() {
        try (Jedis jedis = pool.getResource()) {
            long now = now();
            long max = now + MAX_JOB_DELAY;
            Set<Tuple> nextQueueItems = jedis.zrangeByScoreWithScores(keys.EXPIRATION_QUEUE_KEY, 0, max, 0, 5);

            if (nextQueueItems.isEmpty()) {
                LOG.debug("[{}] Queue empty, next try in {}ms", config.name(), MAX_JOB_DELAY);
                return MAX_JOB_DELAY;
            }

            long next = nextQueueItems.stream()
                    .skip(random.nextInt(nextQueueItems.size()))
                    .findFirst()
                    .map(Tuple::getScore)
                    .map(it -> Math.min(MAX_JOB_DELAY, Math.max(MIN_JOB_DELAY, it.longValue() - now)))
                    .orElse(MAX_JOB_DELAY);
            LOG.debug("[{}] Estimated a next delay of {}ms", config.name(), next);
            return next;
        }
    }

    private synchronized boolean enableJob() {
        if (isEnabled) {
            return false;
        }

        LOG.debug("Enabling expiration queue");
        isEnabled = true;
        scheduleJob(true, estimateNextDelay(), MILLISECONDS);
        return true;
    }

    private synchronized boolean disableJob() {
        if (!isEnabled) {
            return false;
        }

        LOG.debug("Disabling expiration queue");
        isEnabled = false;
        job.cancel(false);
        job = null;
        return true;
    }
}
