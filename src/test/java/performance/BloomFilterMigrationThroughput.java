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

import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
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
    private static final int ITEMS = 100_000;
    private static final int SERVERS = 5;
    private static final long TEST_RUNTIME = 20L;
    private static PrintWriter writer;

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

    public static void main(String[] args) throws Exception {
        writer = new PrintWriter(BloomFilterMigrationThroughput.class.getSimpleName() + ".csv", "UTF-8");
        FilterBuilder builder = new FilterBuilder(ITEMS, 0.02).hashFunction(HashMethod.Murmur3)
                .name("purity")
                .redisBacked(true)
                .redisHost("127.0.0.1")
                .redisPort(6379)
                .redisConnections(10)
                .overwriteIfExists(true)
                .complete();

        BloomFilterMigrationThroughput test;

        test = new BloomFilterMigrationThroughput("Memory Queue, to server", true);
        test.testPerformance(builder, ExpiringBloomFilterRedis.class);

        test = new BloomFilterMigrationThroughput("Redis Queue, to server", true);
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class);

        test = new BloomFilterMigrationThroughput("Memory Queue, from server", false);
        test.testPerformance(builder, ExpiringBloomFilterRedis.class);

        test = new BloomFilterMigrationThroughput("Redis Queue, from server", false);
        test.testPerformance(builder, ExpiringBloomFilterPureRedis.class);

        writer.close();
        System.exit(0);
    }


    public void testPerformance(FilterBuilder builder, Class<? extends ExpiringBloomFilter> type) {
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

        long start = System.currentTimeMillis();
        List<Future<?>> processes = servers.stream().flatMap(this::startServer).collect(toList());

        processes.forEach(process -> {
            try {
                process.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        endTest(servers, start);
    }

    private ExpiringBloomFilter<String> createBloomFilter(FilterBuilder builder, Class<? extends ExpiringBloomFilter> type) {
        FilterBuilder clone = builder.clone().name(createRandomName());
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
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder builder = new StringBuilder(6);
        for (int i = 0; i < builder.capacity(); i += 1) {
            builder.append(chars.charAt(rnd.nextInt(chars.length())));
        }

        return builder.toString();
    }

    private Stream<Future<?>> startServer(ExpiringBloomFilter<String> server) {
        // Schedule migration periodically
        Future<?> future = executor.submit(() -> doMigrateToRedis(server));
        return Stream.of(future);
    }

    private void doMigrateToRedis(ExpiringBloomFilter<String> server) {
        long start = System.nanoTime();
        if (toServer) {
            server.migrateFrom(inMemoryFilter);
        } else {
            inMemoryFilter.migrateFrom(server);
        }
        migHistogram.update(System.nanoTime() - start);
    }

    private void endTest(List<ExpiringBloomFilter<String>> servers, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        LOG.info("Ending Test (Runtime: " + duration + "ms)");

        Snapshot snapshot = migHistogram.getSnapshot();
        LOG.info("Servers     : " + SERVERS);
        LOG.info("Items       : " + ITEMS);
        LOG.info(String.format("Latency Avg : %.4fms", snapshot.getMean() / 1e6d));
        LOG.info(String.format("Latency Min : %.4fms", snapshot.getMin() / 1e6d));
        LOG.info(String.format("Latency Q1  : %.4fms", snapshot.getValue(.25) / 1e6d));
        LOG.info(String.format("Latency Q2  : %.4fms", snapshot.getValue(.5) / 1e6d));
        LOG.info(String.format("Latency Q3  : %.4fms", snapshot.getValue(.75) / 1e6d));
        LOG.info(String.format("Latency Max : %.4fms", snapshot.getMax() / 1e6d));
        writer.println(String.format(Locale.ENGLISH, "'%s',%.4f,%.4f,%.4f,%.4f,%.4f",
                testName,
                (snapshot.getMin() / 1e6d),
                (snapshot.getValue(.25) / 1e6d),
                (snapshot.getValue(.5) / 1e6d),
                (snapshot.getValue(.75) / 1e6d),
                (snapshot.getMax() / 1e6d)));

        servers.forEach(BloomFilter::clear);
    }

    private void addNewItems(ExpiringBloomFilter<String> server, int numberOfItems) {
        IntStream.range(0, numberOfItems)
                .parallel()
                .forEach(i -> {
                    addNewItem(server);
                });
    }

    private void addNewItem(ExpiringBloomFilter<String> server) {
        String item = String.valueOf(rnd.nextInt(ITEMS));
        server.reportRead(item, TimeUnit.NANOSECONDS.convert(TEST_RUNTIME, TimeUnit.SECONDS) + rnd.nextInt(Integer.MAX_VALUE), TimeUnit.NANOSECONDS);
        server.reportWrite(item);
    }
}
