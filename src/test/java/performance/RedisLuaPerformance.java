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
        final int runs = 100;
        final int[] items = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500};
        for (int item : items) {
            final RedisLuaPerformance test = new RedisLuaPerformance(item, runs);
            printResult(item + " els", test.getResult());
        }
        System.exit(0);
    }

    private static void printResult(String label, Snapshot snapshot) {
        final String format = String.format(
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

        final FilterBuilder builder = new FilterBuilder(1_000_000, 0.02)
                .hashFunction(HashProvider.HashMethod.Murmur3)
                .redisBacked(true)
                .redisHost("127.0.0.1")
                .redisPort(6379)
                .redisConnections(10)
                .overwriteIfExists(true)
                .complete();

        final ExpiringBloomFilterPureRedis bloomFilter = new ExpiringBloomFilterPureRedis(builder);
        bloomFilter.disableExpiration();

        for (int run = 0; run < runs; run += 1) {
            for (int i = 0; i < items; i += 1) {
                final String element = String.valueOf(rnd.nextInt());
                bloomFilter.reportRead(element, 5, TimeUnit.MILLISECONDS);
                if (!bloomFilter.reportWrite(element)) {
                    throw new RuntimeException("Did not write " + element);
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            LOG.debug("Start estimation");
            final long started = System.nanoTime();
            while (!bloomFilter.isEmpty()) {
                bloomFilter.expirationHandler();
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
