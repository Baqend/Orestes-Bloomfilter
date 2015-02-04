package orestes.bloomfilter.test.redis;

import orestes.bloomfilter.test.MemoryBFTest;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * A few test to play around with the Jedis client library for Redis
 */
@Ignore
public class RedisTest {

    private static String IP = "192.168.44.131";

    @Ignore
    @Test
    public void testOverflow() {
        Jedis jedis = new Jedis("localhost");
        Pipeline p = jedis.pipelined();
        p.multi();
        for (int i = 0; i < 10000; i++) {
            p.setbit("test", 1, true); //or any other call
        }
        Response<List<Object>> exec = p.exec();
        p.sync();
        exec.get();
    }

    @Test
    public void jedisTest() {
        Jedis jedis = jedis();

        jedis.lpush("meineprivateliste", UUID.randomUUID().toString());
        System.out.println(jedis.lrange("meineprivateliste", 0, -1));

        jedis.watch("foo");
        Transaction t = jedis.multi();
        t.set("foo", "bar");
        Response<String> resp = t.get("foo");
        t.exec();
        System.out.println(resp.get());

        t = jedis.multi();
        int[] positions = new int[]{1, 2, 3};
        for (int position : positions)
            t.getbit("bla", position);
        for (Object obj : t.exec()) {
            System.out.println("Bit : " + (Boolean) obj);
        }

        jedis.set("meinint", Integer.toString(3));
        jedis.incr("meinint");
        int meinint = Integer.valueOf(jedis.get("meinint"));
        System.out.println("Integer: " + meinint);

        jedis.setbit("testbits", 100, true);
        System.out.println(Arrays.toString(jedis.get(SafeEncoder.encode("testbits"))));

        jedis.watch("test");
        if (!jedis.exists("test")) {
            Transaction ta = jedis.multi();
            ta.setbit("test", 100000, false);
            ta.exec();
        }

        Transaction ta = jedis.multi();
        ta.setbit("test", 10, true);
        ta.exec();

        ta = jedis.multi();
        for (int i = 0; i <= 10; i++) {
            ta.getbit("test", i);
        }
        System.out.println(ta.exec());

        jedis.del("perftest");
        long begin = System.nanoTime();
        long bits = 10000000l;
        jedis.setbit("perftest", bits, true);
        System.out.println(jedis.get("perftest"));
        long end = System.nanoTime();
        System.out.println("Fetch " + bits + " bits: " + (end - begin) / 1000000 + " ms");

        Pipeline p = jedis.pipelined();
        p.multi();
        p.set("muh", "mäh");
        p.exec();
        p.sync();
        System.out.println(jedis.get("muh"));

        int size = 10000;
        jedis.setbit("testblob", size, true);
        begin = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            t = jedis.multi();
            jedis.watch("testblob");
            for (int j = 0; j < size / 10; j++) {
                t.setbit("testblob", j, true);
            }
            t.exec();
        }
        end = System.currentTimeMillis();
        System.out.print("Set " + size + " bits in big TAs: ");
        MemoryBFTest.printStat(begin, end, size);

        ArrayList<String> real = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            real.add("Ich bin die OID " + i);
        }
        System.out.print("Set " + size + " bits in small TAs: ");
        long start_add = System.currentTimeMillis();
        for (int i = 0; i < size / 10; i++) {
            jedis.watch("testblob");
            t = jedis.multi();
            //falsePositiveProbability = jedis.pipelined();
            for (int j = 0; j < 10; j++) {
                t.setbit("testblob", (int) (Math.random() * size), true);
            }
            t.exec();
            //falsePositiveProbability.sync();
        }
        long end_add = System.currentTimeMillis();
        MemoryBFTest.printStat(start_add, end_add, size);


        //Pipelining bug if watch is after multi
        Pipeline pipe = jedis.pipelined();
        pipe.watch("myKey");
        pipe.multi();
        pipe.set("myKey", "myVal");
        Response<List<Object>> result = pipe.exec();
        pipe.sync();
        for (Object o : result.get())
            System.out.println(o);

        int ops = 1000;
        begin = System.currentTimeMillis();
        for (int i = 0; i < ops; i++) {
            jedis.set("fastset" + i, "val");
        }
        end = System.currentTimeMillis();
        System.out.print(ops + " set operations: ");
        MemoryBFTest.printStat(begin, end, ops);

        jedis.watch("bits");
        jedis.setbit("bits", 100, true);
        Jedis newJedis = jedis();
        t = newJedis.multi();
        t.getbit("bits", 100);
        List<Object> tResult = t.exec();
        System.out.println("Fetched Bit: " + tResult.get(0));

    }

    @Ignore
    @Test
    public void lua() {
        //No more Lua Scripting -> too slow
        /*Jedis jedis = jedis();
		String script = jedis.scriptLoad("return false");
		System.out.println(jedis.evalsha(script));
		Object result = jedis.eval("return redis.call('incrby',KEYS[1],ARGV[1])", 1, "nonexistent", "10");
		System.out.println(result);
		
		jedis.del("testcounts");
		jedis.del("testbloom");
		result = jedis.eval(CBloomFilterRedisBits.SETANDINCR, 2, "testcounts", "testbloom", "4", "2", "100", "105");
		System.out.println("Incremented: " + result);
		System.out.println("Least Significant Count Bit: " + jedis.getbit("testcounts", 4*101-1));
		System.out.println("Bit in Bloom-Array: " + jedis.getbit("testbloom", 100));
		
		result = jedis.eval(CBloomFilterRedisBits.SETANDDECR, 2, "testcounts","testbloom", "4", "3", "100", "105", "110");
		System.out.println("NonZero: " + result);
		System.out.println("Least Significant Count Bit: " + jedis.getbit("testcounts", 4*101-1));
		System.out.println("Bit in Bloom-Array: " + jedis.getbit("testbloom", 100));*/


    }

    @Test
    public void sharding() {
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        JedisShardInfo si = new JedisShardInfo(IP, 6380);
        shards.add(si);
        ShardedJedis jedis = new ShardedJedis(shards);
        jedis.set("a", "brakdabra");
        System.out.println(jedis.get("a"));
    }


    private Jedis jedis() {
        return new Jedis(IP, 6379);
    }

}
