package orestes.bloomfilter;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class BloomFilters {
	
	/**
	 * A wrapper for a Bloom filter that synchronizes access for concurrent
	 * access similar to Collections.synchronizedCollection.
	 * 
	 * @param bf the Bloom filter
	 * @param <T>
	 *            Type Parameter of the Bloom filter
	 */
	public static <T> BloomFilter<T> synchronizedBloomFilter(BloomFilter<T> bf) {
		return new SynchronizedBloomFilter<>(bf);
	}
	
	public static <T> CBloomFilter<T> synchronizedCBloomFilter(CBloomFilter<T> cbf) {
		return new SynchronizedCBloomFilter<>(cbf);
	}

	
	public static class SynchronizedBloomFilter<T> extends BloomFilter<T> {
		private final BloomFilter<T> bf;

		public SynchronizedBloomFilter(BloomFilter<T> bf) {
			this.bf = bf;
		}

		@Override
		public synchronized void setHashMethod(orestes.bloomfilter.BloomFilter.HashMethod hashMethod) {
			bf.setHashMethod(hashMethod);
		}

		@Override
		public synchronized void setCryptographicHashFunction(String hashFunctionName) {
			bf.setCryptographicHashFunction(hashFunctionName);
		}

		@Override
		public synchronized void setCustomHashFunction(orestes.bloomfilter.BloomFilter.CustomHashFunction chf) {
			bf.setCustomHashFunction(chf);
		}

		@Override
		public synchronized boolean add(byte[] value) {
			return bf.add(value);
		}

		@Override
		public synchronized boolean add(T value) {
			return bf.add(value);
		}

		@Override
		public synchronized List<Boolean> addAll(Collection<T> values) {
			return bf.addAll(values);
		}

		@Override
		public synchronized void clear() {
			bf.clear();
		}

		@Override
		public synchronized boolean contains(byte[] value) {
			return bf.contains(value);
		}

		@Override
		public synchronized boolean contains(T value) {
			return bf.contains(value);
		}

		@Override
		public synchronized boolean containsAll(Collection<T> values) {
			return bf.containsAll(values);
		}

		@Override
		public synchronized BitSet getBitSet() {
			return bf.getBitSet();
		}

		@Override
		public synchronized int[] hash(String value) {
			return bf.hash(value);
		}

		@Override
		public synchronized boolean union(BloomFilter<T> other) {
			return bf.union(other);
		}

		@Override
		public synchronized boolean intersect(BloomFilter<T> other) {
			return bf.intersect(other);
		}

		@Override
		public synchronized boolean isEmpty() {
			return bf.isEmpty();
		}

		@Override
		public synchronized double getFalsePositiveProbability(int n) {
			return bf.getFalsePositiveProbability(n);
		}

		@Override
		public synchronized double getBitsPerElement(int n) {
			return bf.getBitsPerElement(n);
		}

		@Override
		public synchronized double getBitZeroProbability(int n) {
			return bf.getBitZeroProbability(n);
		}

		@Override
		public synchronized int size() {
			return bf.size();
		}

		@Override
		public synchronized int getM() {
			return bf.getM();
		}

		@Override
		public synchronized int getK() {
			return bf.getK();
		}

		@Override
		public synchronized String getCryptographicHashFunctionName() {
			return bf.getCryptographicHashFunctionName();
		}

		@Override
		public synchronized orestes.bloomfilter.BloomFilter.HashMethod getHashMethod() {
			return bf.getHashMethod();
		}

		@Override
		public synchronized Object clone() {
			return bf.clone();
		}

		@Override
		public synchronized int hashCode() {
			return bf.hashCode();
		}

		@Override
		public synchronized boolean equals(Object obj) {
			return bf.equals(obj);
		}

		@Override
		public synchronized String toString() {
			return bf.toString();
		}
	}
	
	/**
	 * * A wrapper for a Counting Bloom filter that synchronizes access for
	 * concurrent access similar to Collections.synchronizedCollection.
	 * 
	 * @param <T>
	 *            Type Parameter of the Counting Bloom filter
	 */
	public static class SynchronizedCBloomFilter<T> extends CBloomFilter<T> {
		private final CBloomFilter<T> cbf;

		public SynchronizedCBloomFilter(CBloomFilter<T> cbf) {
			this.cbf = cbf;
		}

		@Override
		public synchronized void setOverflowHandler(
				orestes.bloomfilter.CBloomFilter.OverflowHandler callback) {
			cbf.setOverflowHandler(callback);
		}

		@Override
		public synchronized boolean add(byte[] value) {
			return cbf.add(value);
		}

		@Override
		public synchronized void remove(byte[] value) {
			cbf.remove(value);
		}

		@Override
		public synchronized void remove(T value) {
			cbf.remove(value);
		}

		@Override
		public synchronized void removeAll(Collection<T> values) {
			cbf.removeAll(values);
		}

		@Override
		public synchronized void setHashMethod(
				orestes.bloomfilter.BloomFilter.HashMethod hashMethod) {
			cbf.setHashMethod(hashMethod);
		}

		@Override
		public synchronized int getC() {
			return cbf.getC();
		}

		@Override
		public synchronized boolean union(BloomFilter<T> other) {
			return cbf.union(other);
		}

		@Override
		public synchronized boolean intersect(BloomFilter<T> other) {
			return cbf.intersect(other);
		}

		@Override
		public synchronized String toString() {
			return cbf.toString();
		}

		@Override
		public synchronized Object clone() {
			return cbf.clone();
		}

		@Override
		public synchronized int hashCode() {
			return cbf.hashCode();
		}

		@Override
		public synchronized void setCryptographicHashFunction(
				String hashFunctionName) {
			cbf.setCryptographicHashFunction(hashFunctionName);
		}

		@Override
		public synchronized boolean equals(Object obj) {
			return cbf.equals(obj);
		}

		@Override
		public synchronized void setCustomHashFunction(
                orestes.bloomfilter.BloomFilter.CustomHashFunction chf) {
			cbf.setCustomHashFunction(chf);
		}

		@Override
		public synchronized void clear() {
			cbf.clear();
		}

		@Override
		public synchronized boolean add(T value) {
			return cbf.add(value);
		}

		@Override
		public synchronized List<Boolean> addAll(Collection<T> values) {
			return cbf.addAll(values);
		}

		@Override
		public synchronized boolean contains(byte[] value) {
			return cbf.contains(value);
		}

		@Override
		public synchronized boolean contains(T value) {
			return cbf.contains(value);
		}

		@Override
		public synchronized boolean containsAll(Collection<T> values) {
			return cbf.containsAll(values);
		}

		@Override
		public synchronized BitSet getBitSet() {
			return cbf.getBitSet();
		}

		@Override
		public int[] hash(String value) {
			return cbf.hash(value);
		}

		@Override
		public synchronized boolean isEmpty() {
			return cbf.isEmpty();
		}

		@Override
		public  double getFalsePositiveProbability(int n) {
			return cbf.getFalsePositiveProbability(n);
		}

		@Override
		public  double getBitsPerElement(int n) {
			return cbf.getBitsPerElement(n);
		}

		@Override
		public  double getBitZeroProbability(int n) {
			return cbf.getBitZeroProbability(n);
		}

		@Override
		public  int size() {
			return cbf.size();
		}

		@Override
		public  int getM() {
			return cbf.getM();
		}

		@Override
		public  int getK() {
			return cbf.getK();
		}

		@Override
		public  String getCryptographicHashFunctionName() {
			return cbf.getCryptographicHashFunctionName();
		}

		@Override
		public  orestes.bloomfilter.BloomFilter.HashMethod getHashMethod() {
			return cbf.getHashMethod();
		}
	}

}
