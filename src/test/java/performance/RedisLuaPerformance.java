package performance;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created on 23.11.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class RedisLuaPerformance {
    private static final Random rnd = new Random(214576);
    private final Histogram histogram;

    /**
     * Logger for the {@link RedisLuaPerformance} class
     */
    private static final Logger LOG = LoggerFactory.getLogger(RedisLuaPerformance.class);

    public static void main(String[] args) {
        int runs = 10;
        int[] items = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000};
        for (int item : items) {
            RedisLuaPerformance test = new RedisLuaPerformance(item, runs);
            printResult(item + "", test.getResult());
        }
        System.exit(0);
    }

    private static void printResult(String label, Snapshot snapshot) {
        String format = String.format(
                Locale.ENGLISH,
                "('%s', %.4f, %.4f, %.4f, %.4f, %.4f),",
                label,
                snapshot.getMin() / 1e6d,
                snapshot.getValue(0.25) / 1e6d,
                snapshot.getMedian() / 1e6d,
                snapshot.getValue(0.75) / 1e6d,
                snapshot.getMax() / 1e6d
        );
        System.out.println(format);
    }

    public RedisLuaPerformance(int items, int runs) {
        histogram = new Histogram(new ExponentiallyDecayingReservoir());

        FilterBuilder builder = new FilterBuilder(1_000_000, 0.02)
                .hashFunction(HashProvider.HashMethod.Murmur3)
                .redisBacked(true)
                .redisHost("127.0.0.1")
                .redisPort(6379)
                .redisConnections(10)
                .overwriteIfExists(true)
                .complete();

        ExpiringBloomFilterPureRedis bloomFilter = new ExpiringBloomFilterPureRedis(builder);
        bloomFilter.disableExpiration();

        for (int run = 0; run < runs; run += 1) {
            for (int i = 0; i < items; i += 1) {
                String element = String.valueOf(rnd.nextInt());
                bloomFilter.reportRead(element, 50, TimeUnit.MILLISECONDS);
                if (!bloomFilter.reportWrite(element)) {
                    throw new RuntimeException("Did not write " + element);
                }
            }

            try {
                Thread.sleep(60);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            LOG.debug("Start estimation");
            long started = System.nanoTime();
            while (!bloomFilter.isEmpty()) {
                bloomFilter.onExpire();
            }
            histogram.update(System.nanoTime() - started);
            LOG.debug("Estimated " + (System.nanoTime() - started));
            bloomFilter.clear();
        }
    }

    Snapshot getResult() {
        return histogram.getSnapshot();
    }
}
