package orestes.bloomfilter.test;

import com.google.common.hash.Hashing;
import junit.framework.TestCase;
import orestes.bloomfilter.HashProvider;
import org.junit.Test;

import java.util.Random;
import java.util.stream.IntStream;

public class MurmurTest {

    @Test
    public void testForEqualsHashes() {
        com.google.common.hash.HashFunction guavaHash = Hashing.murmur3_32();
        Random random = new Random();
        int maxBytes = 100;
        int maxTestsPerByte = 100;

        IntStream.range(0, maxBytes).forEach(i -> {
            IntStream.range(0, maxTestsPerByte).forEach(j -> {

                byte[] input = new byte[i];
                random.nextBytes(input);
                int theirs = guavaHash.hashBytes(input).asInt();
                int ours = HashProvider.murmur3_signed(0, input);
                //System.out.println(i+","+j+":"+theirs+" | " + ours);
                TestCase.assertEquals(theirs, ours);
            });
        });
    }
}
