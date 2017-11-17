package performance;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import org.omg.SendingContext.RunTime;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by erik on 10.10.17.
 */
public class RedisBloomFilterThroughput {
    private static final int ITEMS = 100_000_000;
    private static final int SERVERS = 10;
    private static final int USERS_PER_SERVER = 100;
    private static final int WRITE_PERIOD = 100;
    private static final int READ_PERIOD = 100;
    private static final int TEST_RUNTIME = 20;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);
    private final Random rnd = new Random(214576);
    private final AtomicInteger writes = new AtomicInteger();

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
            .overwriteIfExists(true);

        final RedisBloomFilterThroughput test = new RedisBloomFilterThroughput();

        System.out.println("-------------- Normal Redis --------------");
        test.resetMetrics();
        test.testPerformance(builder, ExpiringBloomFilterRedis.class).join();

        System.out.println("-------------- Pure Redis --------------");
        test.resetMetrics();
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class).join();
        System.exit(0);
    }


    public CompletableFuture<Boolean> testPerformance(FilterBuilder builder, Class<? extends ExpiringBloomFilterRedis> type) {
        List<ExpiringBloomFilter<String>> servers = IntStream.range(0, SERVERS)
            .mapToObj(i -> createBloomFilter(builder, type))
            .collect(toList());

        final long start = System.currentTimeMillis();
        final List<ScheduledFuture<?>> processes = servers.stream().flatMap(this::startUsers).collect(toList());

        final CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        Executors.newSingleThreadScheduledExecutor().schedule(() -> endTest(testResult, processes, servers, start), TEST_RUNTIME, TimeUnit.SECONDS);

        return testResult;
    }

    public void resetMetrics() {
        writes.set(0);
    }

    private ExpiringBloomFilter<String> createBloomFilter(FilterBuilder builder, Class<? extends ExpiringBloomFilterRedis> type) {
        ExpiringBloomFilterRedis<String> result;
        if (type == ExpiringBloomFilterRedis.class) {
            result = new ExpiringBloomFilterRedis<>(builder);
        } else
        if (type == ExpiringBloomFilterPureRedis.class) {
            result = new ExpiringBloomFilterPureRedis(builder);
        } else {
            throw new IllegalArgumentException("Unknown Bloom filter type: " + type);
        }

        result.clear();
        return result;
    }

    private Stream<ScheduledFuture<?>> startUsers(ExpiringBloomFilter<String> server) {
        return IntStream.range(0, USERS_PER_SERVER).mapToObj(userId -> {
            final int randomDelay = rnd.nextInt(1000);

            // report reads and writes periodically
            final ScheduledFuture<?> writeProcess = executor.scheduleAtFixedRate(() -> reportWrite(server), randomDelay,
                WRITE_PERIOD, TimeUnit.MILLISECONDS);

            // read Bloom filter periodically
            final ScheduledFuture<?> bloomFilterReadProcess = executor.scheduleAtFixedRate(
                () -> readBloomFilter(server), randomDelay, READ_PERIOD, TimeUnit.MILLISECONDS);

            return Stream.of(writeProcess, bloomFilterReadProcess);
        }).flatMap(it -> it);
    }

    private void readBloomFilter(ExpiringBloomFilter<String> server) {
        server.getBitSet();
    }

    private void reportWrite(ExpiringBloomFilter<String> server) {
        final String item = getRandomItem();
        server.reportRead(item, 500, TimeUnit.MILLISECONDS);
        server.reportWrite(item);
        writes.getAndIncrement();
    }

    private void endTest(CompletableFuture<Boolean> resultFuture, List<ScheduledFuture<?>> processes, List<ExpiringBloomFilter<String>> servers, long startTime) {
        long endingStarted = (System.currentTimeMillis() - startTime);
        System.out.println("Ending Test (Runtime: " + endingStarted + "ms)");
        processes.forEach(process -> process.cancel(false));
        long duration = (System.currentTimeMillis() - startTime);
        System.out.println("Processes canceled (Runtime: " + duration + "ms)");
        System.out.println("Writes: " + writes.get() + "/" + ((1000 * TEST_RUNTIME * SERVERS * USERS_PER_SERVER) / WRITE_PERIOD) + ", Throughput: " + writes.get() / (duration / 1000) + "/s");

        waitForServersToClear(servers, resultFuture);

        resultFuture.thenAccept((ignored) -> {
//            servers.forEach(server -> server.remove());
            System.out.println("Bloom filter cleanup time: " + ((System.currentTimeMillis() - startTime - duration) / 1000.0) + "s");
        });
    }

    private void waitForServersToClear(List<ExpiringBloomFilter<String>> servers, CompletableFuture<Boolean> future) {
        final boolean serversDone = servers.stream().allMatch(server -> server.getBitSet().isEmpty());
        if (serversDone) {
            future.complete(true);
        } else {
            executor.schedule(() -> waitForServersToClear(servers, future), 500, TimeUnit.MILLISECONDS);
        }
    }

    private String getRandomItem() {
        return String.valueOf(rnd.nextInt(ITEMS));
    }
}
