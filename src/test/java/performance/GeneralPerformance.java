package performance;

import orestes.bloomfilter.CountingBloomFilter;
import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.HashProvider.HashMethod;
import orestes.bloomfilter.cachesketch.ExpiringBloomFilterRedis;
import orestes.bloomfilter.redis.CountingBloomFilterRedis;
import performance.BFHashUniformity.Randoms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class GeneralPerformance {
    private static int ops = 1_000_000;
    private static int from = 0;
    private static int to = 12;

    public static void main(String... args) throws Exception {
/*        testOp((in, b) -> b.contains(in), "contains");
        testOp((in, b) -> b.add(in), "add");
        testOp((in, b) -> b.remove(in), "remove");

        testOp((in, b) -> b.add(in), "badd");
        testOp((in, b) -> b.contains(in), "bcontains");
        testOp((in, b) -> b.getEstimatedPopulation(), "bpop");*/

        /*testOpExp((in, b) -> b.contains(in), "contains");
        testOpExp((in, b) -> b.reportRead(in, 100, TimeUnit.SECONDS), "reportRead");
        testOpExp((in, b) -> b.reportWrite(in), "reportWrite");
        testOpExp((in, b) -> b.getRemainingTTL(in, TimeUnit.MILLISECONDS), "getRemainingTTL");
        testOpExp((in, b) -> b.getEstimatedPopulation(), "cardinality");
        testOpExp((in, b) -> b.getBytes(), "dump", 100_000);
        testOpExp((in, b) -> b.getBytes(), "dump", 10_000);
        testOpExp((in, b) -> b.contains(in), "contains");*/


        plotX(from, to);
        for (int size : Arrays.asList(10_000, 100_000)) {
            testOpExp((in, b) -> b.contains(in), "contains" + size, size);
            testOpExp((in, b) -> b.reportRead(in, 100, TimeUnit.SECONDS), "reportRead" + size, size);
            testOpExp((in, b) -> b.reportWrite(in), "reportWrite" + size, size);
            testOpExp((in, b) -> b.getRemainingTTL(in, TimeUnit.MILLISECONDS), "getRemainingTTL" + size, size);
            testOpExp((in, b) -> b.getEstimatedPopulation(), "cardinality" + size, size);
            testOpExp((in, b) -> b.getBytes(), "dump" + size, size);
            testOpExp((in, b) -> b.contains(in), "contains" + size, size);
        }
    }

    private static void testOpExp(BiConsumer<String, ExpiringBloomFilterRedis<String>> op, String name) throws Exception {
        testOpExp(op, name, 100_000);
    }

    private static void testOpExp(BiConsumer<String, ExpiringBloomFilterRedis<String>> op, String name, int size) throws Exception {
        Graph g = new Graph();

        List<String> input = Randoms.BYTES.generate(ops, 1)
            .get(0)
            .stream()
            .map(Randoms::fromBytes)
            .collect(Collectors.toList());
        for (int i = from; i < to; i++) {
            for (int j = 0; j < 5; j++) {
                //System.out.println((int) (Math.pow(2, i)));
                ExpiringBloomFilterRedis<String> b = new ExpiringBloomFilterRedis<>(
                    new FilterBuilder(size, 5).hashFunction(HashMethod.Murmur3)
                        .redisBacked(true)
                        .name("")
                        .redisHost("134.100.11.230").redisPort(6379)
                        //.addReadSlave("localhost", 6380)
                        //.addReadSlave("localhost", 6381)
                        //.addReadSlave("localhost", 6382)
                        .overwriteIfExists(true).redisConnections((int) (Math.pow(2, i))).complete());
                ExecutorService exec = Executors.newFixedThreadPool((int) (Math.pow(2, i)));
                warmup(exec);
                for (int k = 0; k < size / 10; k++) {
                    b.add(input.get(k));
                }
                long start = System.currentTimeMillis();
                for (String in : input) {
                    exec.submit(() -> op.accept(in, b));
                }
                exec.shutdown();
                exec.awaitTermination(200, TimeUnit.SECONDS);
                long end = System.currentTimeMillis();
                g.record(name, String.valueOf(ops * 1000d / (end - start)));
                b.remove();
            }
        } System.out.println(g.generateData());
    }

    private static void plotX(int from, int to) {
        String result = "x = [";
        for (int i = from; i < to; i++) {
            result += (int) Math.pow(2, i) + ",";
        }
        result = result.substring(0, result.lastIndexOf(",")) + "]";
        System.out.println(result);
    }

    private static void testOp(BiConsumer<byte[], CountingBloomFilter<String>> op, String name) throws Exception {
        Graph g = new Graph();

        for (int i = 0; i < 7; i++) {
            System.out.println((int) (Math.pow(2, i)));
            CountingBloomFilterRedis<String> b = new CountingBloomFilterRedis<>(
                new FilterBuilder(100_000, 5).hashFunction(HashMethod.Murmur2)
                    .redisBacked(true)
                    .name("")
                    .redisHost("localhost").redisPort(6379)
                    //.addReadSlave("localhost", 6380)
                    //.addReadSlave("localhost", 6381)
                    //.addReadSlave("localhost", 6382)
                    .overwriteIfExists(true).redisConnections((int) (Math.pow(2, i))).complete());
            ExecutorService exec = Executors.newFixedThreadPool((int) (Math.pow(2, i)));
            warmup(exec);
            List<byte[]> input = Randoms.BYTES.generate(ops, 1).get(0);
            for (byte[] in : input) {
                b.add(in);
                b.add(in);
                b.add(in);
                b.add(in);
            }
            long start = System.currentTimeMillis();
            for (byte[] in : input) {
                exec.submit(() -> op.accept(in, b));
            }
            exec.shutdown();
            exec.awaitTermination(50, TimeUnit.SECONDS);
            long end = System.currentTimeMillis();
            g.record(name, String.valueOf(ops * 1000d / (end - start)));
        }
        System.out.println(g.generateData());
    }


    private static void warmup(ExecutorService exec) throws Exception {
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
                result += entry.getKey() + "= [";
                for (String val : entry.getValue()) {
                    result += val + ",";
                }
                result = result.substring(0, result.lastIndexOf(",")) + "];";
            }
            return result;
        }
    }

}
