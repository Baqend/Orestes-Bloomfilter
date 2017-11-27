package performance;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created on 20.11.2017.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class BloomFilterMigrationThroughput {
    private static final int ITEMS = 1_000_000;
    private static final int SERVERS = 1;
    private static final long TEST_RUNTIME = 20L;
    private static final int COOL_DOWN_TIME = 60;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);
    private final Random rnd = new Random(214576);
    private final Histogram migHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    private final String testName;
    private final boolean toServer;
    private ExpiringBloomFilterMemory<String> inMemoryFilter;

    /**
     * Logger for the {@link BloomFilterMigrationThroughput} class
     */
    private static final Logger LOG = LoggerFactory.getLogger(BloomFilterMigrationThroughput.class);

    private BloomFilterMigrationThroughput(String name, boolean toServer) {
        LOG.info("Running Test: " + name);
        this.testName = name;
        this.toServer = toServer;
    }

    public static void main(String[] args) {
        final FilterBuilder builder = new FilterBuilder(ITEMS, 0.02).hashFunction(HashMethod.Murmur3)
                .name("purity")
                .redisBacked(true)
                .redisHost("127.0.0.1")
                .redisPort(6379)
                .redisConnections(10)
                .overwriteIfExists(true)
                .complete();

        BloomFilterMigrationThroughput test;

//        test = new BloomFilterMigrationThroughput("Memory Queue, to server", true);
//        test.testPerformance(builder, ExpiringBloomFilterRedis.class).join();

        test = new BloomFilterMigrationThroughput("Redis Queue, to server", true);
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class).join();

//        test = new BloomFilterMigrationThroughput("Memory Queue, from server", false);
//        test.testPerformance(builder, ExpiringBloomFilterRedis.class).join();

//        test = new BloomFilterMigrationThroughput("Redis Queue, from server", false);
//        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class).join();

        System.exit(0);
    }


    public CompletableFuture<Boolean> testPerformance(FilterBuilder builder, Class<? extends ExpiringBloomFilter> type) {
        builder.pool().safelyDo(jedis -> jedis.flushAll());
        LOG.debug("Flushed Redis");

        inMemoryFilter = new ExpiringBloomFilterMemory<>(builder);
        if (toServer) addNewItems(inMemoryFilter, ITEMS);
        LOG.debug("Created in-memory Bloom filter");

        List<ExpiringBloomFilter<String>> servers = IntStream.range(0, SERVERS)
                .mapToObj(i -> createBloomFilter(builder, type))
                .collect(toList());
        if (!toServer) addNewItems(servers.get(0), ITEMS);
        LOG.debug("Created " + SERVERS + " server Bloom filters");

        final long start = System.currentTimeMillis();
        final List<Future<?>> processes = servers.stream().flatMap(this::startServer).collect(toList());

        final CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        Executors.newSingleThreadScheduledExecutor().schedule(() -> endTest(testResult, processes, servers, start), TEST_RUNTIME, TimeUnit.SECONDS);

        return testResult;
    }

    private ExpiringBloomFilter<String> createBloomFilter(FilterBuilder builder, Class<? extends ExpiringBloomFilter> type) {
        final FilterBuilder clone = builder.clone().name(createRandomName());
        ExpiringBloomFilter<String> result;
        if (type == ExpiringBloomFilterRedis.class) {
            result = new ExpiringBloomFilterRedis<>(clone);
        } else if (type == ExpiringBloomFilterPureRedis.class) {
            result = new ExpiringBloomFilterPureRedis(clone);
        } else {
            throw new IllegalArgumentException("Unknown Bloom filter type: " + type);
        }

        return result;
    }

    private String createRandomName() {
        final String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        final StringBuilder builder = new StringBuilder(6);
        for (int i = 0; i < builder.capacity(); i += 1) {
            builder.append(chars.charAt(rnd.nextInt(chars.length())));
        }

        return builder.toString();
    }

    private Stream<Future<?>> startServer(ExpiringBloomFilter<String> server) {
        // Schedule migration periodically
        final Future<?> future = executor.submit(() -> doMigrateToRedis(server));
        return Stream.of(future);
    }

    private void doMigrateToRedis(ExpiringBloomFilter<String> server) {
        final long start = System.nanoTime();
        if (toServer) {
            server.migrateFrom(inMemoryFilter);
        } else {
            inMemoryFilter.migrateFrom(server);
        }
        migHistogram.update(System.nanoTime() - start);
    }

    private void endTest(CompletableFuture<Boolean> resultFuture, List<Future<?>> processes, List<ExpiringBloomFilter<String>> servers, long startTime) {
        long endingStarted = System.currentTimeMillis() - startTime;
        LOG.info("Ending Test (Runtime: " + endingStarted + "ms)");
        processes.forEach(process -> process.cancel(false));
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Processes canceled (Runtime: " + duration + "ms)");

        final Snapshot snapshot = migHistogram.getSnapshot();
        LOG.info("Servers     : " + SERVERS);
        LOG.info("Count       : " + migHistogram.getCount());
        LOG.info("Throughput  : " + (migHistogram.getCount() / (duration / 1000)) + "/s");
        LOG.info("Latency Avg : " + (snapshot.getMean() / 1e6d) + "ms");
        LOG.info("Latency Min : " + (snapshot.getMin() / 1e6d) + "ms");
        LOG.info("Latency Q1  : " + (snapshot.getValue(.25) / 1e6d) + "ms");
        LOG.info("Latency Q2  : " + (snapshot.getValue(.5) / 1e6d) + "ms");
        LOG.info("Latency Q3  : " + (snapshot.getValue(.75) / 1e6d) + "ms");
        LOG.info("Latency Max : " + (snapshot.getMax() / 1e6d) + "ms");

        waitForServersToClear(servers, resultFuture);

        resultFuture.thenAccept((ignored) -> {
            LOG.info("Bloom filter cleanup time: " + ((System.currentTimeMillis() - startTime - duration) / 1000.0) + "s");
        });
    }

    private void waitForServersToClear(List<ExpiringBloomFilter<String>> servers, CompletableFuture<Boolean> future) {
        final boolean serversDone = servers.stream().allMatch(BloomFilter::isEmpty);
        if (serversDone) {
            future.complete(true);
        } else {
            Executors.newSingleThreadScheduledExecutor().schedule(() -> waitForServersToClear(servers, future), 500, TimeUnit.MILLISECONDS);
        }
    }

    private void addNewItems(ExpiringBloomFilter<String> server, int numberOfItems) {
        IntStream.range(0, numberOfItems)
                .parallel()
                .forEach(i -> {
                    addNewItem(server);
                });
    }

    private void addNewItem(ExpiringBloomFilter<String> server) {
        final String item = String.valueOf(rnd.nextInt(ITEMS));
        server.reportRead(item, TimeUnit.NANOSECONDS.convert(TEST_RUNTIME, TimeUnit.SECONDS) + rnd.nextInt(Integer.MAX_VALUE), TimeUnit.NANOSECONDS);
        server.reportWrite(item);
    }
}
