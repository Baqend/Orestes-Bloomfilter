package orestes.bloomfilter.memory;

import orestes.bloomfilter.FilterBuilder;
import orestes.bloomfilter.BloomFilter;

import java.util.BitSet;

public class BloomFilterMemory<T> implements BloomFilter<T> {
    private final FilterBuilder config;
    protected BitSet bloom;

    public BloomFilterMemory(FilterBuilder config) {
        config.complete();
        bloom = new BitSet(config.size());
        this.config = config;
    }

    @Override
    public FilterBuilder config() {
        return config;
    }

    @Override
    public synchronized boolean add(byte[] element) {
        boolean added = false;
        for (int position : hash(element)) {
            if (!getBit(position)) {
                added = true;
                setBit(position, true);
            }
        }
        return added;
    }

    @Override
    public synchronized void clear() {
        bloom.clear();
    }

    @Override
    public synchronized boolean contains(byte[] element) {
        for (int position : hash(element))
            if (!getBit(position))
                return false;
        return true;
    }

    protected boolean getBit(int index) {
        return bloom.get(index);
    }

    protected void setBit(int index, boolean to) {
        bloom.set(index, to);
    }

    @Override
    public BitSet getBitSet() {
        return bloom;
    }


    @Override
    public synchronized boolean union(BloomFilter<T> other) {
        if (compatible(this, other)) {
            bloom.or(other.getBitSet());
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean intersect(BloomFilter<T> other) {
        if (compatible(this, other)) {
            bloom.and(other.getBitSet());
            return true;
        }
        return false;
    }


    @Override
    public synchronized boolean isEmpty() {
        return bloom.isEmpty();
    }


    @Override
    @SuppressWarnings("unchecked")
    public synchronized BloomFilter<T> clone() {
        BloomFilterMemory<T> o = null;
        try {
            o = (BloomFilterMemory<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        o.bloom = (BitSet) bloom.clone();
        //TODO clone config
        return o;
    }


    @Override
    public synchronized String toString() {
        return asString();
    }


    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BloomFilterMemory)) return false;

        BloomFilterMemory that = (BloomFilterMemory) o;

        if (bloom != null ? !bloom.equals(that.bloom) : that.bloom != null) return false;
        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) return false;

        return true;
    }

}
