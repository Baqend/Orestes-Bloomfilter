package performance;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;

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
    private static final int TEST_RUNTIME = 120;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);
    private final Random rnd = new Random(214576);
    private final Counter migCounter = new Counter();
    private final Histogram migHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    private final String testName;
    private final boolean toServer;
    private ExpiringBloomFilterMemory<String> inMemoryFilter;

    private BloomFilterMigrationThroughput(String name, boolean toServer) {
        System.out.println("-------------- " + name + " --------------");
        this.testName = name;
        this.toServer = toServer;
    }

    public static void main(String[] args) {
        int m = 100_000;
        int k = 10;

        System.err.println("Please make sure to have Redis running on 127.0.0.1:6379.");
        final FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3)
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


    public CompletableFuture<Boolean> testPerformance(FilterBuilder builder, Class<? extends ExpiringBloomFilterRedis> type) {
        System.out.print("Flushing Redis ... ");
        builder.pool().safelyDo(jedis -> jedis.flushAll());
        System.out.println("done.");

        System.out.print("Creating in-memory Bloom filter ... ");
        inMemoryFilter = new ExpiringBloomFilterMemory<>(builder);
        if (toServer) addNewItems(inMemoryFilter, ITEMS);
        System.out.println("done.");

        System.out.print("Creating server Bloom filters ... ");
        List<ExpiringBloomFilter<String>> servers = IntStream.range(0, SERVERS)
                .mapToObj(i -> createBloomFilter(builder, type))
                .collect(toList());
        if (!toServer) addNewItems(servers.get(0), ITEMS);
        System.out.println("done.");

        final long start = System.currentTimeMillis();
        final List<ScheduledFuture<?>> processes = servers.stream().flatMap(this::startUsers).collect(toList());

        final CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        Executors.newSingleThreadScheduledExecutor().schedule(() -> endTest(testResult, processes, servers, start), TEST_RUNTIME, TimeUnit.SECONDS);

        return testResult;
    }

    private ExpiringBloomFilter<String> createBloomFilter(FilterBuilder builder, Class<? extends ExpiringBloomFilterRedis> type) {
        ExpiringBloomFilterRedis<String> result;
        if (type == ExpiringBloomFilterRedis.class) {
            result = new ExpiringBloomFilterRedis<>(builder);
        } else if (type == ExpiringBloomFilterPureRedis.class) {
            result = new ExpiringBloomFilterPureRedis(builder);
        } else {
            throw new IllegalArgumentException("Unknown Bloom filter type: " + type);
        }

        return result;
    }

    private Stream<ScheduledFuture<?>> startUsers(ExpiringBloomFilter<String> server) {
        final int randomDelay = rnd.nextInt(1000);

        // Schedule migration periodically
        final ScheduledFuture<?> writeProcess = executor.scheduleWithFixedDelay(
                () -> doMigrateToRedis(server), randomDelay, 1, TimeUnit.MILLISECONDS);

        return Stream.of(writeProcess);
    }

    private void doMigrateToRedis(ExpiringBloomFilter<String> server) {
        final long start = System.nanoTime();
        if (toServer) {
            server.migrateFrom(inMemoryFilter);
        } else {
            inMemoryFilter.migrateFrom(server);
        }
        migCounter.inc();
        migHistogram.update(System.nanoTime() - start);
    }

    private void endTest(CompletableFuture<Boolean> resultFuture, List<ScheduledFuture<?>> processes, List<ExpiringBloomFilter<String>> servers, long startTime) {
        long endingStarted = System.currentTimeMillis() - startTime;
        System.out.println("Ending Test (Runtime: " + endingStarted + "ms)");
        processes.forEach(process -> process.cancel(false));
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Processes canceled (Runtime: " + duration + "ms)");

        final Snapshot snapshot = migHistogram.getSnapshot();
        System.out.println("Count       : " + migCounter.getCount());
        System.out.println("Throughput  : " + (migCounter.getCount() / (duration / 1000)) + "/s");
        System.out.println("Latency Avg : " + (snapshot.getMean() / 1e6d) + "ms");
        System.out.println("Latency Min : " + (snapshot.getMin() / 1e6d) + "ms");
        System.out.println("Latency Q1  : " + (snapshot.getValue(.25) / 1e6d) + "ms");
        System.out.println("Latency Q2  : " + (snapshot.getValue(.5) / 1e6d) + "ms");
        System.out.println("Latency Q3  : " + (snapshot.getValue(.75) / 1e6d) + "ms");
        System.out.println("Latency Max : " + (snapshot.getMax() / 1e6d) + "ms");

        waitForServersToClear(servers, resultFuture);

        resultFuture.thenAccept((ignored) -> {
            System.out.println("Bloom filter cleanup time: " + ((System.currentTimeMillis() - startTime - duration) / 1000.0) + "s");
        });
    }

    private void waitForServersToClear(List<ExpiringBloomFilter<String>> servers, CompletableFuture<Boolean> future) {
        final boolean serversDone = servers.stream().allMatch(it -> it.getBitSet().isEmpty());
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
        server.reportRead(item, TEST_RUNTIME + 1, TimeUnit.SECONDS);
        server.reportWrite(item);
    }
}
