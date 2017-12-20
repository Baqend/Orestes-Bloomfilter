package orestes.bloomfilter.memory;


import orestes.bloomfilter.FilterBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CountingBloomFilter32<T> extends CountingBloomFilterMemory<T>{
    private int[] counters;
    private static final long MAX = ((long) Integer.MAX_VALUE) * 2 + 1;

    public CountingBloomFilter32(FilterBuilder config) {
        config.countingBits(32).complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counters = new int[config.size()];
    }

    @Override
    protected long increment(int index) {
        if(Integer.toUnsignedLong(counters[index]) == MAX) {
            overflowHandler.run();
            return MAX;
        }
        return Integer.toUnsignedLong(++counters[index]);
    }

    @Override
    protected long decrement(int index) {
        if(counters[index] == 0)
            return 0;
        return Integer.toUnsignedLong(--counters[index]);
    }

    @Override
    protected long count(int index) {
        return Integer.toUnsignedLong(counters[index]);
    }

    @Override
    protected void set(int index, long newValue) {
        counters[index] = (int) newValue;
    }

    @Override
    public Map<Integer, Long> getCountMap() {
        Map<Integer, Long> result = new HashMap<>();
        for (int i = 0; i < counters.length; i++) {
            long count = Integer.toUnsignedLong(counters[i]);
            if (count > 0) {
                result.put(i, count);
            }
        }

        return result;
    }

    @Override
    public void clear() {
        filter.clear();
        this.counters = new int[counters.length];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof CountingBloomFilter32)) { return false; }
        if (!super.equals(o)) { return false; }
        CountingBloomFilter32<?> that = (CountingBloomFilter32<?>) o;
        return Objects.equals(counters, that.counters);
    }

}
