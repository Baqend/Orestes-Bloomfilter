package orestes.bloomfilter.cachesketch;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.TimeMap;
import orestes.bloomfilter.redis.MessagePackEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

    private final String exportQueueScript = loadLuaScript("exportQueue.lua");
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
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
        pool.safelyDo((jedis) -> {
            // Delete all used fields from Redis
            jedis.del(keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY, keys.TTL_KEY);
        });
    }

    @Override
    public void remove() {
        super.remove();
        if (job != null) {
            job.cancel(true);
            job = null;
        }
    }

    @Override
    public void addToQueue(String item, long remaining, TimeUnit timeUnit) {
        pool.safelyDo(p -> {
            boolean done;
            do {
                int[] positions = hash(item.getBytes());
                final byte[] member = msgPack.encodeItem(item, positions);
                done = p.zadd(keys.EXPIRATION_QUEUE_KEY.getBytes(), now() + MILLISECONDS.convert(remaining, timeUnit), member) == 1;
            } while (!done);
        });
        triggerExpirationHandling(remaining, timeUnit);
    }

    /**
     * Handles expiring items from the expiration queue.
     *
     * @return true if successful, false otherwise.
     */
    synchronized public boolean onExpire() {
        final long now = now();
        LOG.debug("[" + config.name() + "] Expiring items ... " + now);
        long expiredItems = pool.safelyReturn((jedis) -> (long) jedis.evalsha(
                exportQueueScript, 3,
                // Keys:
                keys.EXPIRATION_QUEUE_KEY, keys.COUNTS_KEY, keys.BITS_KEY,
                // Args:
                String.valueOf(now)
        ));
        LOG.debug("[" + config.name() + "] Script expired " + expiredItems + " items within " + (now() - now) + "ms");
        return true;
    }


    @Override
    public TimeMap<String> getExpirationMap() {
        return pool.safelyReturn(p -> p.zrangeWithScores(keys.EXPIRATION_QUEUE_KEY.getBytes(), 0, -1))
                .stream()
                .collect(TimeMap.collectMillis(
                        tuple -> msgPack.decodeItem(tuple.getBinaryElement()),
                        tuple -> (long) tuple.getScore()
                ));
    }

    @Override
    public void setExpirationMap(TimeMap<String> map) {
        pool.safelyDo((jedis) -> {
            final Pipeline pipeline = jedis.pipelined();
            final AtomicInteger ctr = new AtomicInteger(0);
            map.forEach((item, expiration) -> {
                final int[] positions = hash(item);
                pipeline.zadd(keys.EXPIRATION_QUEUE_KEY.getBytes(), expiration, msgPack.encodeItem(item, positions));

                if (ctr.incrementAndGet() >= 1000) {
                    ctr.set(0);
                    pipeline.sync();
                }
            });
            pipeline.sync();
        });
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
    synchronized private void triggerExpirationHandling(long delay, TimeUnit unit) {
        if (!isEnabled) return;
        long delayInMilliseconds = TimeUnit.MILLISECONDS.convert(delay, unit);
        long currentDelay = job.getDelay(TimeUnit.MILLISECONDS);
        if (currentDelay > delayInMilliseconds + MIN_JOB_DELAY) {
            scheduleJob(false, delay, unit);
        }
    }

    /**
     * Schedules a new job to work the queue.
     * @param shouldNotCancel Whether the last job is about to end or needs to be canceled
     * @param delay When to schedule the new job
     * @param unit The time unit for the delay
     */
    synchronized private void scheduleJob(boolean shouldNotCancel, long delay, TimeUnit unit) {
        ScheduledFuture<?> currentJob = job;
        if (!shouldNotCancel && currentJob != null) {
            LOG.debug("[" + config.name() + "] Cancel active job");
            currentJob.cancel(false);
        }
        LOG.debug("[" + this.config.name() + "] Scheduled the next expiration job in " + TimeUnit.MILLISECONDS.convert(delay, unit) + "ms");
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
        return pool.safelyReturn(p -> {
            final long now = now();
            long max = now + MAX_JOB_DELAY;
            Set<Tuple> nextQueueItems = p.zrangeByScoreWithScores(keys.EXPIRATION_QUEUE_KEY, 0, max, 0, 5);

            if (nextQueueItems.isEmpty()) {
                LOG.debug("[" + config.name() + "] Queue empty, next try in " + MAX_JOB_DELAY + "ms");
                return MAX_JOB_DELAY;
            }

            final long next = nextQueueItems.stream()
                    .skip(random.nextInt(nextQueueItems.size()))
                    .findFirst()
                    .map(Tuple::getScore)
                    .map(it -> Math.min(MAX_JOB_DELAY, Math.max(MIN_JOB_DELAY, it.longValue() - now)))
                    .orElse(MAX_JOB_DELAY);
            LOG.debug("[" + config.name() + "] Estimated a next delay of " + next + "ms");
            return next;
        });
    }

    private boolean enableJob() {
        if (isEnabled) {
            return false;
        }

        LOG.debug("Enabling expiration queue");
        isEnabled = true;
        scheduleJob(true, estimateNextDelay(), MILLISECONDS);
        return true;
    }

    private boolean disableJob() {
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
