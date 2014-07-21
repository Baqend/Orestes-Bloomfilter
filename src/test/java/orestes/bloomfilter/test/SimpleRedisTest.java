package orestes.bloomfilter.test;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.redis.BloomFilterRedis;
import redis.clients.jedis.Jedis;

public class SimpleRedisTest {

    public static void main(String[] args) {

        Jedis client = new Jedis("localhost");

        BloomFilterRedis<String> filter = BloomFilterRedis.createPopulationFilter(client, "testing", 100_000, 0.1, BloomFilter.HashMethod.Murmur);

        filter.useConnection(new Jedis("localhost"));
        filter.add("Testing");

        if (filter.getPopulation() == 1) System.out.println("Added");
        else System.out.println(" Oops, didn't add??");

        if (filter.contains("Testing")) System.out.println("Found");
        else System.out.println(" Oops, didn't find??");
    }
}
