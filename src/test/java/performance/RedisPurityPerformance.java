package performance;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilter;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterMemory;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterPureRedis;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import orestes.bloomfilter.test.MemoryBFTest;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
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


        FilterBuilder builder = new FilterBuilder(m, k)
            .hashFunction(HashMethod.Murmur3)
            .name("purity")
            .redisBacked(true)
            .redisHost("127.0.0.1")
            .redisPort(6379)
            .redisConnections(10)
            .overwriteIfExists(true);

        try {
            ExpiringBloomFilterPureRedis pure = new ExpiringBloomFilterPureRedis(builder);
            ExpiringBloomFilterRedis<String> unpure = new ExpiringBloomFilterRedis<>(builder);
//            final ExpiringBloomFilterMemory<String> unpure = new ExpiringBloomFilterMemory<>(builder);

            dumbAdds(pure);
            dumbAdds(unpure);
            dumbAdds(pure);
            dumbAdds(unpure);

            System.exit(0);
        } catch (Exception e) {
            System.err.println("Please make sure to have Redis running on 127.0.0.1:6379.");
        }
    }

    private static void dumbAdds(ExpiringBloomFilter<String> b) throws Exception {
        b.clear();
        System.out.println(b.isEmpty());
        System.out.println(b.getBitSet().length());
        Random r = new Random();
        boolean[] stop = {false};

        // Start a reading thread
        Thread reader = new Thread(() -> {
            int[] lastCount = {-1, -1, -1, -1, -1};
            while (true) {
                BitSet bitSet = b.getBitSet();
                lastCount[0] = lastCount[1];
                lastCount[1] = lastCount[2];
                lastCount[2] = lastCount[3];
                lastCount[3] = lastCount[4];
                lastCount[4] = bitSet.length();

                if (stop[0] && (lastCount[0] == lastCount[1]) && (lastCount[0] == lastCount[2]) && (lastCount[0] == lastCount[3]) && (lastCount[0] == lastCount[4])) {
//                if (stop[0]) {
                    System.out.println(b.isEmpty());
                    System.out.println(bitSet.length());
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


        HashSet<String> processed = new HashSet<>();
        System.err.println("Threads started.");
        long start = System.currentTimeMillis();
        for (int j = 0; j < CONCURRENT_USERS; j += 1) {
            Thread thread = new Thread(() -> {
                while (true) {
                    String item;
                    do {
                        item = String.valueOf(r.nextInt());
                    } while (processed.contains(item));
                    processed.add(item);
                    b.reportRead(item, 500, TimeUnit.MILLISECONDS);
                    if (!b.reportWrite(item)) {
                        throw new RuntimeException("Should have to invalidate " + item);
                    }

                    if (stop[0]) {
                        break;
                    }
                }
            });
            thread.start();
        }
        Thread.sleep(10000);
        long beforeJoin = System.currentTimeMillis();
        stop[0] = true;
        System.err.println("Threads stopped.");
        reader.join();
        long end = System.currentTimeMillis();
        System.err.println("Queue emptied after " + (end - beforeJoin) + " ms");
        MemoryBFTest.printStat(start, end, processed.size());
    }

}
