package orestes.bloomfilter.memory;


import orestes.bloomfilter.FilterBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CountingBloomFilter64<T> extends CountingBloomFilterMemory<T>{
    private long[] counters;
    private static final long MAX = Long.MAX_VALUE;

    public CountingBloomFilter64(FilterBuilder config) {
        config.countingBits(64).complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counters = new long[config.size()];
    }

    @Override
    protected long increment(int index) {
        if(counters[index] == MAX)
            return MAX;
        return ++counters[index];
    }

    @Override
    protected long decrement(int index) {
        if(counters[index] == 0)
            return 0;
        return --counters[index];
    }

    @Override
    protected long count(int index) {
        return counters[index];
    }

    @Override
    protected void set(int index, long newValue) {
        counters[index] = newValue;
    }

    @Override
    public Map<Integer, Long> getCountMap() {
        Map<Integer, Long> result = new HashMap<>();
        for (int i = 0; i < counters.length; i++) {
            long count = counters[i];
            if (count > 0) {
                result.put(i, count);
            }
        }

        return result;
    }

    @Override
    public void clear() {
        filter.clear();
        this.counters = new long[counters.length];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof CountingBloomFilter64)) { return false; }
        if (!super.equals(o)) { return false; }
        CountingBloomFilter64<?> that = (CountingBloomFilter64<?>) o;
        return Objects.equals(counters, that.counters);
    }

}
