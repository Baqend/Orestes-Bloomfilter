package performance;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import orestes.bloomfilter.test.MemoryBFTest;

import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created on 27.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class RedisPurityPerformance {
    private static final int ITEMS = 100_000_000;
    private static final int CONCURRENT_USERS = 10;

    public static void main(String[] args) throws Exception {
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

        ExpiringBloomFilterPureRedis pure = new ExpiringBloomFilterPureRedis(builder);
        ExpiringBloomFilterRedis<String> unpure = new ExpiringBloomFilterRedis<>(builder);

        dumbAdds(pure);
        dumbAdds(unpure);
        dumbAdds(pure);
        dumbAdds(unpure);

        System.err.println("System exits NOW");
        System.exit(0);
    }

    private static void dumbAdds(ExpiringBloomFilterRedis<String> b) throws Exception {
        b.clear();
        Random r = new Random();
        final int[] i = {0};
        final boolean[] stop = {false};

        // Start a reading thread
        final Thread reader = new Thread(() -> {
            int[] lastCount = {-1, -1, -1, -1, -1};
            while (true) {
                final BitSet bitSet = b.getBitSet();
                lastCount[0] = lastCount[1];
                lastCount[1] = lastCount[2];
                lastCount[2] = lastCount[3];
                lastCount[3] = lastCount[4];
                lastCount[4] = bitSet.length();

                if (stop[0] && (lastCount[0] == lastCount[1]) && (lastCount[0] == lastCount[2]) && (lastCount[0] == lastCount[3]) && (lastCount[0] == lastCount[4])) {
                    System.err.println("Queue is now empty.");
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        reader.start();


        System.err.println("Threads started.");
        long start = System.currentTimeMillis();
        for (int j = 0; j < CONCURRENT_USERS; j += 1) {
            final Thread thread = new Thread(() -> {
                while (true) {
                    final String item = String.valueOf(r.nextInt(ITEMS));
                    b.reportRead(item, 500, TimeUnit.MILLISECONDS);
                    b.reportWrite(item);
                    i[0] += 1;
                    if (stop[0]) {
                        break;
                    }
                }
            });
            thread.start();
            System.err.println("Definitely started user " + j);
        }
        Thread.sleep(10000);
        long beforeJoin = System.currentTimeMillis();
        stop[0] = true;
        System.err.println("Threads stopped.");
        reader.join();
        long end = System.currentTimeMillis();
        System.err.println("Queue emptied after " + (end - beforeJoin) + " ms");
        MemoryBFTest.printStat(start, end, i[0]);
    }

}
