package orestes.bloomfilter.test.cachesketch;

import com.google.common.collect.Lists;
import orestes.bloomfilter.cachesketch.ExpirationQueueMemory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Created on 04.10.17.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class ExpirationQueueMemoryTest {

    private ExpirationQueueMemory<String> queue;
    private int handlerCallsCount;

    @Before
    public void setUp() throws Exception {
        handlerCallsCount = 0;
        queue = new ExpirationQueueMemory<>(it -> handlerCallsCount++);
    }

    @Test
    public void testAddElements() throws Exception {
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
        assertTrue(queue.addTTL("demo", 1, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 1, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 1, TimeUnit.SECONDS));
        assertEquals(3, queue.size());
        assertTrue(queue.contains("demo"));
        assertTrue(queue.contains("foo"));
        assertTrue(queue.contains("bar"));

        Thread.sleep(1500);
        assertEquals(0, queue.size());
        assertEquals(3, handlerCallsCount);
        assertFalse(queue.contains("demo"));
        assertFalse(queue.contains("foo"));
        assertFalse(queue.contains("bar"));
    }

    @Test
    public void testIterable() throws Exception {
        assertTrue(queue.addTTL("demo", 12, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("foo", 11, TimeUnit.SECONDS));
        assertTrue(queue.addTTL("bar", 10, TimeUnit.SECONDS));

        int calls = 0;
        for (String item : queue) {
            calls++;
        }
        assertEquals(3, calls);

        Set<String> elements = new HashSet<>(Lists.newArrayList(queue));
        assertEquals(new HashSet<>(Arrays.asList("bar", "demo", "foo")), elements);
    }

    @Test
    public void testAddDuplicateElements() throws Exception {
        assertEquals(0, queue.size());

        assertFalse(queue.contains("demo"));

        assertTrue(queue.addTTL("demo", 1, TimeUnit.MILLISECONDS));
        assertTrue(queue.addTTL("demo", 2, TimeUnit.SECONDS));


        assertEquals(2, queue.size());
        assertTrue(queue.contains("demo"));

        Thread.sleep(1500);
        assertEquals(1, queue.size());
        assertTrue(queue.contains("demo"));

        Thread.sleep(1500);
        assertEquals(0, queue.size());
        assertFalse(queue.contains("demo"));
    }
}
