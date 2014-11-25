package performance;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.redis.BloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import performance.BFHashUniformity.Randoms;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class GeneralPerformance {
    private static int ops = 200_000;
    private static int threads = 8;

    public static void main(String... args) throws Exception{
        testOp((in, b) -> b.contains(in), "contains");
        testOp((in, b) -> b.add(in), "add");
        testOp((in, b) -> b.remove(in), "remove");

        testOp((in, b) -> b.add(in), "badd");
        testOp((in, b) -> b.contains(in), "bcontains");
        testOp((in, b) -> b.getEstimatedPopulation(), "bpop");
    }

    private static void testOp(BiConsumer<byte[],CountingBloomFilter<String>> op, String name) throws Exception{
        Graph g = new Graph();

        for (int i = 0; i < 7; i++) {
            System.out.println((int) (Math.pow(2, i)));
            CountingBloomFilter<String> b = new CountingBloomFilterRedis<>(new FilterBuilder(100_000, 5).hashFunction(HashMethod.Murmur2)
                    .redisBacked(true)
                    .name("")
                    .redisHost("localhost")
                    .redisPort(6379)
                            //.addReadSlave("localhost", 6380)
                            //.addReadSlave("localhost", 6381)
                            //.addReadSlave("localhost", 6382)
                    .overwriteIfExists(true)
                    .redisConnections((int) (Math.pow(2, i))).complete());
            ExecutorService exec = Executors.newFixedThreadPool((int) (Math.pow(2, i)));
            warmup(exec);
            List<byte[]> input = Randoms.BYTES.generate(ops, 1).get(0);
            for (byte[] in : input) {
                b.add(in); b.add(in);
                b.add(in); b.add(in);
            }
            long start = System.currentTimeMillis();
            for (byte[] in : input) {
                exec.submit(() -> op.accept(in,b));
            }
            exec.shutdown();
            exec.awaitTermination(50, TimeUnit.SECONDS);
            long end = System.currentTimeMillis();
            g.record(name, String.valueOf(ops * 1000d / (end - start)));
        }
        System.out.println(g.generateData());
    }

    private static void testOpB(BiConsumer<byte[],BloomFilter<String>> op, String name) throws Exception{
        Graph g = new Graph();

        for (int i = 0; i < 7; i++) {
            System.out.println((int) (Math.pow(2, i)));
            BloomFilter<String> b = new BloomFilterRedis<>(new FilterBuilder(100_000, 5).hashFunction(HashMethod.Murmur2)
                    .redisBacked(true)
                    .name("")
                    .redisHost("localhost")
                    .redisPort(6379)
                            //.addReadSlave("localhost", 6380)
                            //.addReadSlave("localhost", 6381)
                            //.addReadSlave("localhost", 6382)
                    .overwriteIfExists(true)
                    .redisConnections((int) (Math.pow(2, i))).complete());
            ExecutorService exec = Executors.newFixedThreadPool(threads);
            warmup(exec);
            List<byte[]> input = Randoms.BYTES.generate(ops, 1).get(0);
            for (byte[] in : input) {
                b.add(in); b.add(in);
                b.add(in); b.add(in);
            }
            long start = System.currentTimeMillis();
            for (byte[] in : input) {
                exec.submit(() -> op.accept(in,b));
            }
            exec.shutdown();
            exec.awaitTermination(50, TimeUnit.SECONDS);
            long end = System.currentTimeMillis();
            g.record(name, String.valueOf(ops * 1000d / (end - start)));
        }
        System.out.println(g.generateData());
    }


    private static void warmup(ExecutorService exec) throws Exception{
        CountDownLatch latch = new CountDownLatch(64);
        for (int i = 0; i < 64; i++) {
            exec.submit(latch::countDown);
        }
        latch.await();
    }


    public static class Graph {
        private Map<String, Queue<String>> graph;

        public Graph() {
            this.graph = new ConcurrentHashMap<>();
        }

        public void record(String key, String value) {
            graph.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>());
            graph.get(key).add(value);
        }

        public String generateData() {
            String result = "";
            for (Entry<String, Queue<String>> entry : graph.entrySet()) {
                result += entry.getKey() + ":= {";
                for (String val : entry.getValue()) {
                    result += val + ",";
                }
                result = result.substring(0, result.lastIndexOf(",")) + "}";
            }
            return result;
        }
    }

}
