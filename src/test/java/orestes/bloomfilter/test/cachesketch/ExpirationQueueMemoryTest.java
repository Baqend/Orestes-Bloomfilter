package orestes.bloomfilter.test.cachesketch;

import com.google.common.collect.Lists;
import orestes.bloomfilter.cachesketch.ExpirationQueue;
import orestes.bloomfilter.cachesketch.ExpirationQueueMemory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created on 04.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class ExpirationQueueMemoryTest {

    @Test
    public void testAddElements() throws Exception {
        final ExpirationQueue<String> queue = new ExpirationQueueMemory<>(str -> {
        });

        assertEquals(0, queue.size());

        assertFalse(queue.contains("demo"));
        assertFalse(queue.contains("foo"));
        assertFalse(queue.contains("bar"));

        assertTrue(queue.addTTL("demo", 10, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 10, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 10, TimeUnit.SECONDS));

        assertEquals(3, queue.size());

        assertTrue(queue.contains("demo"));
        assertTrue(queue.contains("foo"));
        assertTrue(queue.contains("bar"));
    }

    @Test
    public void testRemoveElements() throws Exception {
        final ExpirationQueue<String> queue = new ExpirationQueueMemory<>(str -> {
        });

        assertTrue(queue.addTTL("demo", 10, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 10, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 10, TimeUnit.SECONDS));

        assertEquals(3, queue.size());

        assertTrue(queue.remove("demo"));
        assertTrue(queue.remove("foo"));
        assertTrue(queue.remove("bar"));

        assertEquals(0, queue.size());

        assertFalse(queue.contains("demo"));
        assertFalse(queue.contains("foo"));
        assertFalse(queue.contains("bar"));
    }

    @Test
    public void testElementsAreRemoved() throws Exception {
        final int[] calls = {0};
        final ExpirationQueue<String> queue = new ExpirationQueueMemory<>(str -> {
            calls[0]++;
        });

        assertTrue(queue.addTTL("demo", 1, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 1, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 1, TimeUnit.SECONDS));
        assertEquals(3, queue.size());
        assertTrue(queue.contains("demo"));
        assertTrue(queue.contains("foo"));
        assertTrue(queue.contains("bar"));

        Thread.sleep(1500);
        assertEquals(0, queue.size());
        assertEquals(3, calls[0]);
        assertFalse(queue.contains("demo"));
        assertFalse(queue.contains("foo"));
        assertFalse(queue.contains("bar"));
    }

    @Test
    public void testIterable() throws Exception {
        final ExpirationQueue<String> queue = new ExpirationQueueMemory<>(str -> {
        });

        assertTrue(queue.addTTL("demo", 12, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 11, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 10, TimeUnit.SECONDS));

        int calls = 0;
        for (String item : queue) {
            calls++;
        }
        assertEquals(3, calls);

        final ArrayList<String> list = Lists.newArrayList(queue);
        assertEquals(Arrays.asList("bar", "demo", "foo"), list);
    }
}
