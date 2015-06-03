package orestes.bloomfilter.memory;


import orestes.bloomfilter.FilterBuilder;

import java.util.Objects;

public class CountingBloomFilter64<T> extends CountingBloomFilterMemory<T>{
    private long[] counters;

    public CountingBloomFilter64(FilterBuilder config) {
        config.complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counters = new long[config.size()];
    }

    @Override
    protected long increment(int index) {
        return ++counters[index];
    }

    @Override
    protected long decrement(int index) {
        return --counters[index];
    }

    @Override
    protected long count(int index) {
        return counters[index];
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
