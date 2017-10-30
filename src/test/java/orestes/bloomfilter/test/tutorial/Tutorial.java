package orestes.bloomfilter.test.tutorial;

import com.google.gson.JsonElement;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.json.BloomFilterConverter;
import orestes.bloomfilter.memory.CountingBloomFilterMemory;
import orestes.bloomfilter.redis.helper.RedisPool;
import orestes.bloomfilter.test.MemoryBFTest;
import orestes.bloomfilter.test.redis.RedisBFTest;
import org.apache.commons.lang3.RandomStringUtils;
import performance.BFHashUniformity;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Tutorial {

    public static void main(String[] args) throws Exception {
        //redisBenchmark();
        BFHashUniformity.testHashing();
    }

    public static void redisBenchmark() {
        int connections = 100;
        RedisPool pool = RedisPool.builder().host("localhost").port(6379).redisConnections(connections).build();
        ExecutorService exec = Executors.newFixedThreadPool(connections);

        int rounds = 200_000;
        int perThread = rounds / connections;
        CompletableFuture[] futures = new CompletableFuture[connections];
        Long start = System.currentTimeMillis();
        for (int i = 0; i < connections; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                pool.safelyDo(jedis -> {
                    for(int j = 0; j < perThread; j++) {
                        jedis.set("key" + Math.random(), RandomStringUtils.random(50));
                    }
                });
            }, exec);
        }
        CompletableFuture.allOf(futures).join();
        Long stop = System.currentTimeMillis();
        System.out.printf("Time to complete %s sets: %s ms\n", rounds, stop - start);
    }

    public static void regularBF() {
        //Create a Bloom filter that has a false positive rate of 0.1 when containing 1000 elements
        BloomFilter<String> bf = new FilterBuilder(1000, 0.1).buildBloomFilter();

        //Add a few elements
        bf.add("Just");
        bf.add("a");
        bf.add("test.");

        //Test if they are contained
        print(bf.contains("Just")); //true
        print(bf.contains("a")); //true
        print(bf.contains("test.")); //true

        //Test with a non-existing element
        print(bf.contains("WingDangDoodel")); //false

        //Add 300 elements
        for (int i = 0; i < 300; i++) {
            String element = "Element " + i;
            bf.add(element);
        }

        //test for false positives
        for (int i = 300; i < 1000; i++) {
            String element = "Element " + i;
            if (bf.contains(element)) {
                print(element); //two elements: 440, 669
            }
        }
        //Compare with the expected amount
        print(bf.getFalsePositiveProbability(303) * 700); //1.74

        //Estimate cardinality/population
        print(bf.getEstimatedPopulation()); //303.6759147801151

        //Clone the Bloom filter
        bf.clone();
        //Reset it
        bf.clear();
        //add in Bulk
        List<String> bulk = Arrays.asList("one", "two", "three");
        bf.addAll(bulk);
        print(bf.containsAll(bulk)); //true

        //Create a more customized Bloom filter
        BloomFilter<Integer> bf2 = new FilterBuilder()
                .expectedElements(6666) //elements
                .size(10000) //bits to use
                .hashFunction(HashMethod.Murmur3) //our hash
                .buildBloomFilter();

        print("#Hashes:" + bf2.getHashes()); //2
        print(FilterBuilder.optimalK(6666, 10000)); //you can also do these calculations yourself

        //Create two Bloom filters with equal parameters
        BloomFilter<String> one = new FilterBuilder(100, 0.1).buildBloomFilter();
        BloomFilter<String> other = new FilterBuilder(100, 0.1).buildBloomFilter();
        one.add("this");
        other.add("that");
        one.union(other);
        print(one.contains("this")); //true
        print(one.contains("that")); //true

        other.add("this");
        other.add("boggles");
        one.intersect(other);
        print(one.contains("this")); //true
        print(one.contains("boggles")); //false

    }

    public static void countingBF() throws MalformedURLException {
        //Create a Counting Bloom filter that has a FP rate of 0.01 when 1000 are inserted
        //and uses 4 bits for counting
        CountingBloomFilter<String> cbf = new FilterBuilder(1000, 0.01).buildCountingBloomFilter();
        cbf.add("http://google.com");
        cbf.add("http://twitter.com");
        print(cbf.contains("http://google.com")); //true
        print(cbf.contains("http://twitter.com")); //true

        //What only the Counting Bloom filter can do:
        cbf.remove("http://google.com");
        print(cbf.contains("http://google.com")); //false

        //Use the Memory Bloom filter explicitly:
        FilterBuilder fb = new FilterBuilder(1000, 0.01).countingBits(4);
        CountingBloomFilterMemory<String> filter = new CountingBloomFilterMemory<>(fb);
        filter.setOverflowHandler(() -> print("ups"));

        for (int i = 1; i < 20; i++) {
            print("Round " + i);
            filter.add("http://example.com"); //Causes onOverflow() in Round >= 16
        }

        //See the inner structure
        CountingBloomFilter<String> small = new FilterBuilder(3, 0.2)
                .countingBits(4)
                .buildCountingBloomFilter();
        small.add("One");
        small.add("Two");
        small.add("Three");
        print(small.toString());

    }

    public static void redisBF() {
        String host = "localhost";
        int port = 6379;
        String filterName = "normalbloomfilter";
        //Open a Redis-backed Bloom filter
        BloomFilter<String> bfr = new FilterBuilder(1000, 0.01)
                .name(filterName) //use a distinct name
                .redisBacked(true)
                .redisHost(host) //Default is localhost
                .redisPort(port) //Default is standard 6379
                .buildBloomFilter();
        bfr.add("cow");

        //Open the same Bloom filter from anywhere else
        BloomFilter<String> bfr2 = new FilterBuilder(1000, 0.01)
                .name(filterName) //load the same filter
                .redisBacked(true)
                .buildBloomFilter();
        bfr2.add("bison");

        print(bfr.contains("cow")); //true
        print(bfr.contains("bison")); //true
    }

    public static void redisCBF() {
        //Open a Redis-backed Counting Bloom filter
        CountingBloomFilter<String> cbfr = new FilterBuilder(10000, 0.01)
                .name("myfilter")
                .overwriteIfExists(true) //instead of loading it, overwrite it if it's already there
                .redisBacked(true)
                .buildCountingBloomFilter();
        cbfr.add("cow");

        //Open a second Redis-backed Bloom filter with a new connection
        CountingBloomFilter<String> bfr2 = new FilterBuilder(10000, 0.01)
                .name("myfilter") //this time it will be load it
                .redisBacked(true)
                .buildCountingBloomFilter();
        bfr2.add("bison");
        bfr2.remove("cow");

        print(cbfr.contains("bison")); //true
        print(cbfr.contains("cow")); //false
    }

    public static void jsonBF() {
        BloomFilter<String> bf = new FilterBuilder().expectedElements(50).falsePositiveProbability(0.1).buildBloomFilter();
        bf.add("Ululu");
        JsonElement json = BloomFilterConverter.toJson(bf);
        print(json); //{"size":240,"hashes":4,"HashMethod":"MD5","bits":"AAAAEAAAAACAgAAAAAAAAAAAAAAQ"}
        BloomFilter<String> otherBf = BloomFilterConverter.fromJson(json);
        print(otherBf.contains("Ululu")); //true
    }

    public static void customHash() {
        BloomFilter<String> bf = new FilterBuilder(1000, 0.01)
                .hashFunction((value, m, k) -> null)
                .buildBloomFilter();
    }

    public static void testPerformance() throws UnknownHostException, IOException {
        //Test the performance of the in-memory Bloom filter
        BloomFilter<String> bf = new FilterBuilder(100_000, 0.01).hashFunction(HashMethod.Murmur3).buildBloomFilter();
        MemoryBFTest.benchmark(bf, "Normal Bloom Filter", 1_000_000);

        //And the Redis-backed BF
        String IP = "192.168.44.132";
        String filterName = "normalbloomfilter";
        //Open a Redis-backed Bloom filter
        CountingBloomFilter<String> cbfr = new FilterBuilder(10000, 0.01).redisBacked(true).buildCountingBloomFilter();
        MemoryBFTest.benchmark(cbfr, "Redis Test", 10_000);
        cbfr.remove();


        List<BloomFilter<String>> bfs = new ArrayList<>();
        CountingBloomFilter<String> first = new FilterBuilder(1000, 0.01)
                .redisBacked(true)
                .buildCountingBloomFilter();
        bfs.add(first);
        for (int i = 1; i < 10; i++) {
            bfs.add(first.clone());
        }
        RedisBFTest.concurrentBenchmark(bfs, 2000);
    }


    private static <T> void print(T msg) {
        System.out.println(msg);
    }

}
