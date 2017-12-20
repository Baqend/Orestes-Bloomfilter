package performance;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.AbstractExpiringBloomFilterRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.*;
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
    private final Histogram readHistogram;
    private final Histogram writeHistogram;
    private final String testName;
    private static PrintWriter writer;

    public RedisBloomFilterThroughput(String name) {
        testName = name;
        System.err.println("-------------- " + name + " --------------");

        this.readHistogram = new Histogram(new ExponentiallyDecayingReservoir());
        this.writeHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    }

    public static void main(String[] args) throws Exception {
        writer = new PrintWriter(RedisBloomFilterThroughput.class.getSimpleName() + ".csv", "UTF-8");

        int m = 100_000;
        int k = 10;

        System.err.println("Please make sure to have Redis running on 127.0.0.1:6379.");
        FilterBuilder builder = new FilterBuilder(m, k).hashFunction(HashMethod.Murmur3)
            .name("purity")
            .redisBacked(true)
            .redisHost("127.0.0.1")
            .redisPort(6379)
            .redisConnections(10)
            .overwriteIfExists(true)
            .complete();

        RedisBloomFilterThroughput test;

        builder.pool().safelyDo(jedis -> jedis.flushAll());
        test = new RedisBloomFilterThroughput("Redis Queue 1");
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class).join();

        builder.pool().safelyDo(jedis -> jedis.flushAll());
        test = new RedisBloomFilterThroughput("Memory Queue 1");
        test.testPerformance(builder, ExpiringBloomFilterRedis.class).join();

        builder.pool().safelyDo(jedis -> jedis.flushAll());
        test = new RedisBloomFilterThroughput("Redis Queue 2");
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class).join();

        builder.pool().safelyDo(jedis -> jedis.flushAll());
        test = new RedisBloomFilterThroughput("Memory Queue 2");
        test.testPerformance(builder, ExpiringBloomFilterRedis.class).join();

        writer.close();
        System.exit(0);
    }


    public CompletableFuture<Boolean> testPerformance(FilterBuilder builder, Class<? extends AbstractExpiringBloomFilterRedis> type) {
        List<ExpiringBloomFilter<String>> servers = IntStream.range(0, SERVERS)
            .mapToObj(i -> createBloomFilter(i, builder, type))
            .collect(toList());

        long start = System.currentTimeMillis();
        List<ScheduledFuture<?>> processes = servers.stream().flatMap(this::startUsers).collect(toList());

        CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        Executors.newSingleThreadScheduledExecutor().schedule(() -> endTest(testResult, processes, servers, start), TEST_RUNTIME, TimeUnit.SECONDS);

        return testResult;
    }

    private ExpiringBloomFilter<String> createBloomFilter(int i, FilterBuilder builder, Class<? extends AbstractExpiringBloomFilterRedis> type) {
        ExpiringBloomFilter<String> result;
        if (type == ExpiringBloomFilterRedis.class) {
            result = new ExpiringBloomFilterRedis<>(builder.clone().name("Server " + (i + 1)));
        } else
        if (type == ExpiringBloomFilterPureRedis.class) {
            result = new ExpiringBloomFilterPureRedis(builder.clone().name("Server " + (i + 1)));
        } else {
            throw new IllegalArgumentException("Unknown Bloom filter type: " + type);
        }

        result.clear();
        return result;
    }

    private Stream<ScheduledFuture<?>> startUsers(ExpiringBloomFilter<String> server) {
        return IntStream.range(0, USERS_PER_SERVER).mapToObj(userId -> {
            int randomDelay = rnd.nextInt(1000);

            // report reads and writes periodically
            ScheduledFuture<?> writeProcess = executor.scheduleAtFixedRate(
                () -> doReportWrite(server), randomDelay, WRITE_PERIOD, TimeUnit.MILLISECONDS);

            // read Bloom filter periodically
            ScheduledFuture<?> bloomFilterReadProcess = executor.scheduleAtFixedRate(
                () -> doReadBloomFilter(server), randomDelay, READ_PERIOD, TimeUnit.MILLISECONDS);

            return Stream.of(writeProcess, bloomFilterReadProcess);
        }).flatMap(it -> it);
    }

    private void doReadBloomFilter(ExpiringBloomFilter<String> server) {
        long start = System.nanoTime();
        server.getBitSet();
        readHistogram.update(System.nanoTime() - start);
    }

    private void doReportWrite(ExpiringBloomFilter<String> server) {
        long start = System.nanoTime();
        String item = getRandomItem();
        server.reportRead(item, 500, TimeUnit.MILLISECONDS);
        server.reportWrite(item);
        writeHistogram.update(System.nanoTime() - start);
    }

    private void endTest(CompletableFuture<Boolean> resultFuture, List<ScheduledFuture<?>> processes, List<ExpiringBloomFilter<String>> servers, long startTime) {
        long endingStarted = (System.currentTimeMillis() - startTime);
        System.err.println("Ending Test (Runtime: " + endingStarted + "ms)");
        processes.forEach(process -> process.cancel(false));
        long duration = (System.currentTimeMillis() - startTime);
        System.err.println("Processes canceled (Runtime: " + duration + "ms)");
        System.err.println("Writes: " + writeHistogram.getCount() + "/" + ((1000 * TEST_RUNTIME * SERVERS * USERS_PER_SERVER) / WRITE_PERIOD) + ", Throughput: " + writeHistogram.getCount() / (duration / 1000) + "/s");

        dumpHistogram("Reads", readHistogram);
        dumpHistogram("Writes", writeHistogram);
        waitForServersToClear(servers, resultFuture);

        resultFuture.thenAccept((ignored) -> {
//            servers.forEach(server -> server.remove());
            System.err.println("Bloom filter cleanup time: " + ((System.currentTimeMillis() - startTime - duration) / 1000.0) + "s");
        });
    }

    private void dumpHistogram(String name, Histogram histogram) {
        Snapshot snapshot = histogram.getSnapshot();
        String format = String.format(
                Locale.ENGLISH,
                "'%s', %.4f, %.4f, %.4f, %.4f, %.4f",
                testName + " " + name,
                snapshot.getMin() / 1e6d,
                snapshot.getValue(0.25) / 1e6d,
                snapshot.getMedian() / 1e6d,
                snapshot.getValue(0.75) / 1e6d,
                snapshot.getMax() / 1e6d
        );
        writer.println(format);
    }

    private void waitForServersToClear(List<ExpiringBloomFilter<String>> servers, CompletableFuture<Boolean> future) {
        boolean serversDone = servers.stream().allMatch(server -> server.getBitSet().isEmpty());
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
