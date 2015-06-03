package orestes.bloomfilter.memory;


import orestes.bloomfilter.FilterBuilder;

import java.util.Objects;

public class CountingBloomFilter16<T> extends CountingBloomFilterMemory<T>{
    private short[] counters;

    public CountingBloomFilter16(FilterBuilder config) {
        config.complete();
        this.config = config;
        this.filter = new BloomFilterMemory<>(config.clone());
        this.counters = new short[config.size()];
    }


    @Override
    protected long increment(int index) {
        return Short.toUnsignedLong(++counters[index]);
    }

    @Override
    protected long decrement(int index) {
        return Short.toUnsignedLong(--counters[index]);
    }

    @Override
    protected long count(int index) {
        return Short.toUnsignedLong(counters[index]);
    }

    @Override
    public void clear() {
        filter.clear();
        this.counters = new short[counters.length];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof CountingBloomFilter16)) { return false; }
        if (!super.equals(o)) { return false; }
        CountingBloomFilter16<?> that = (CountingBloomFilter16<?>) o;
        return Objects.equals(counters, that.counters);
    }

}
