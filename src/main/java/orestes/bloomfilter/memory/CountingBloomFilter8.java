package orestes.bloomfilter.memory;


import orestes.bloomfilter.FilterBuilder;

import java.util.Objects;

public class CountingBloomFilter8<T> extends CountingBloomFilterMemory<T>{
    private byte[] counters;

    public CountingBloomFilter8(FilterBuilder config) {
        config.complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counters = new byte[config.size()];
    }

    @Override
    protected long increment(int index) {
        return Byte.toUnsignedLong(++counters[index]);
    }

    @Override
    protected long decrement(int index) {
        return Byte.toUnsignedLong(--counters[index]);
    }

    @Override
    protected long count(int index) {
        return Byte.toUnsignedLong(counters[index]);
    }

    @Override
    public void clear() {
        filter.clear();
        this.counters = new byte[counters.length];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof CountingBloomFilter8)) { return false; }
        if (!super.equals(o)) { return false; }
        CountingBloomFilter8<?> that = (CountingBloomFilter8<?>) o;
        return Objects.equals(counters, that.counters);
    }

}
