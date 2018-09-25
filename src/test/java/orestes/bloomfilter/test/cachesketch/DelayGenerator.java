package orestes.bloomfilter.test.cachesketch;

import orestes.bloomfilter.test.cachesketch.DelayGenerator.DelayNamePair;

import java.util.Iterator;
import java.util.Random;

/**
 * Created on 2018-09-25.
 *
 * @author Konstantin Simon Maria Möllers
 */
public class DelayGenerator implements Iterable<DelayNamePair> {
    private final int numberOfDelaysToGenerate;

    public DelayGenerator(int numberOfDelaysToGenerate) {
        this.numberOfDelaysToGenerate = numberOfDelaysToGenerate;
    }

    @Override
    public Iterator<DelayNamePair> iterator() {
        Random r = new Random(numberOfDelaysToGenerate);
        return new Iterator<DelayNamePair>() {
            private int k;

            @Override
            public boolean hasNext() {
                return k < numberOfDelaysToGenerate;
            }

            @Override
            public DelayNamePair next() {
                k += 1;
                return new DelayNamePair((r.nextInt(numberOfDelaysToGenerate) * 10) + 1000, String.valueOf(k));
            }
        };
    }

    public class DelayNamePair {
        private final int delay;
        private final String item;

        public DelayNamePair(int delay, String item) {
            this.delay = delay;
            this.item = item;
        }

        public int getDelay() {
            return delay;
        }

        public String getItem() {
            return item;
        }
    }
}
