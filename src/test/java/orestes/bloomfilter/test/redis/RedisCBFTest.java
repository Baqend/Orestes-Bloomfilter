package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class RedisCBFTest {
    private static String name = "RedisCBFTest";


    @Ignore
    @Test
    public void concurrencyTests() throws InterruptedException {
        int objects = 500;
        int m = 100;
        int k = 10;
        int threads = 4;
        AtomicBoolean false_negative = new AtomicBoolean(false);
        List<String> real = Collections.synchronizedList(new ArrayList<>(objects));
        for (int i = 0; i < objects; i++) {
            real.add("object " + i);
        }
        // final CBloomFilter<String> cbfr = new CBloomFilter<String>(10000, 10, 4);
        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            int id = i;
            Runnable run = () -> {
                CountingBloomFilter<String> filter = new FilterBuilder(m, k).name(name).redisBacked(true).buildCountingBloomFilter();
                for (int j = 0; j < objects; j++) {
                    String str = real.get((int) (Math.random() * objects));
                    String before = filter.getBitSet().toString();
                    filter.add(str);
                    String between = filter.getBitSet().toString();
                    if (!filter.contains(str)) {
                        //False Negative
                        System.out.println("[Thread " + id + "]: False Negative: " + str + " not contained");
                        System.out.println("Before: " + before);
                        System.out.println("Between: " + between);
                        System.out.println(Arrays.toString(filter.hash(str)));
                        false_negative.set(true);
                    }
                    filter.remove(str);
                    if (filter.contains(str)) {
                        //False Positive
                        System.out.println("[Thread " + id + "]: False Positive: " + str + " still contained");
                    }
                }
            };
            Thread thread = new Thread(run);
            thread.start();
            ts[i] = thread;
        }
        for (int i = 0; i < threads; i++) {
            ts[i].join();
        }
        assertTrue(!false_negative.get());
    }
}
