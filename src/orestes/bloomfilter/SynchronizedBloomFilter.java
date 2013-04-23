package orestes.bloomfilter;

import java.util.BitSet;
import java.util.Collection;
import java.util.Random;
import java.util.zip.Checksum;

/**
 * A wrapper for a Bloom filter that synchronizes access for concurrent access
 * similar to Collections.synchronizedCollection.
 * 
 * @param <T>
 *            Type Parameter of the Bloom filter
 */
public  class SynchronizedBloomFilter<T> extends BloomFilter<T> {
	private BloomFilter<T> bf;

	public SynchronizedBloomFilter(BloomFilter<T> bf) {
		this.bf = bf;
	}

	@Override
	public synchronized  void setHashMethod(
			orestes.bloomfilter.BloomFilter.HashMethod hashMethod) {

		bf.setHashMethod(hashMethod);
	}

	@Override
	public synchronized  void setCryptographicHashFunction(String hashFunctionName) {

		bf.setCryptographicHashFunction(hashFunctionName);
	}

	@Override
	public synchronized  void setCusomHashFunction(
			orestes.bloomfilter.BloomFilter.CustomHashFunction chf) {

		bf.setCusomHashFunction(chf);
	}

	@Override
	public synchronized  boolean add(byte[] value) {

		return bf.add(value);
	}

	@Override
	public synchronized  boolean add(T value) {

		return bf.add(value);
	}

	@Override
	public synchronized  void addAll(Collection<T> values) {

		bf.addAll(values);
	}

	@Override
	public synchronized  void clear() {

		bf.clear();
	}

	@Override
	public synchronized  boolean contains(byte[] value) {

		return bf.contains(value);
	}

	@Override
	public synchronized  boolean contains(T value) {

		return bf.contains(value);
	}

	@Override
	public synchronized  boolean containsAll(Collection<T> values) {

		return bf.containsAll(values);
	}

	@Override
	protected synchronized  boolean getBit(int index) {

		return bf.getBit(index);
	}

	@Override
	protected synchronized  void setBit(int index) {

		bf.setBit(index);
	}

	@Override
	protected synchronized  void setBit(int index, boolean to) {

		bf.setBit(index, to);
	}

	@Override
	public synchronized  BitSet getBitSet() {

		return bf.getBitSet();
	}

	@Override
	public synchronized  int[] hash(String value) {

		return bf.hash(value);
	}

	@Override
	protected synchronized  int[] hashCarterWegman(byte[] value) {

		return bf.hashCarterWegman(value);
	}

	@Override
	protected synchronized  int[] hashRNG(byte[] value) {

		return bf.hashRNG(value);
	}

	@Override
	protected synchronized  int[] hashSecureRNG(byte[] value) {

		return bf.hashSecureRNG(value);
	}

	@Override
	protected synchronized  int[] hashMagnus(byte[] value) {

		return bf.hashMagnus(value);
	}

	@Override
	protected synchronized  int[] hashCRC(byte[] value) {

		return bf.hashCRC(value);
	}

	@Override
	protected synchronized  int[] hashAdler(byte[] value) {

		return bf.hashAdler(value);
	}

	@Override
	protected synchronized  int[] hashChecksum(byte[] value, Checksum cs) {

		return bf.hashChecksum(value, cs);
	}

	@Override
	protected synchronized  int[] hashLCG(byte[] value) {

		return bf.hashLCG(value);
	}

	@Override
	protected synchronized  int[] hashMurmur(byte[] value) {

		return bf.hashMurmur(value);
	}

	@Override
	protected synchronized  int rejectionSample(int random) {

		return bf.rejectionSample(random);
	}

	@Override
	protected synchronized  int[] hashCrypt(byte[] value) {

		return bf.hashCrypt(value);
	}

	@Override
	protected synchronized  int[] hash(byte[] value) {

		return bf.hash(value);
	}

	@Override
	public synchronized  boolean union(BloomFilter<T> other) {

		return bf.union(other);
	}

	@Override
	public synchronized  boolean intersect(BloomFilter<T> other) {

		return bf.intersect(other);
	}

	@Override
	protected synchronized  boolean compatible(BloomFilter<T> bloomFilter,
			BloomFilter<T> other) {

		return bf.compatible(bloomFilter, other);
	}

	@Override
	public synchronized  boolean isEmpty() {

		return bf.isEmpty();
	}

	@Override
	public synchronized  double getFalsePositiveProbability(int n) {

		return bf.getFalsePositiveProbability(n);
	}

	@Override
	public synchronized  double getBitsPerElement(int n) {

		return bf.getBitsPerElement(n);
	}

	@Override
	public synchronized  double getBitZeroProbability(int n) {

		return bf.getBitZeroProbability(n);
	}

	@Override
	public synchronized  int size() {

		return bf.size();
	}

	@Override
	public synchronized  int getM() {

		return bf.getM();
	}

	@Override
	public synchronized  int getK() {

		return bf.getK();
	}

	@Override
	public synchronized  String getCryptographicHashFunctionName() {

		return bf.getCryptographicHashFunctionName();
	}

	@Override
	public synchronized  orestes.bloomfilter.BloomFilter.HashMethod getHashMethod() {

		return bf.getHashMethod();
	}

	@Override
	protected synchronized  Random getRandom() {

		return bf.getRandom();
	}

	@Override
	public synchronized  Object clone() {

		return bf.clone();
	}

	@Override
	public synchronized  int hashCode() {

		return bf.hashCode();
	}

	@Override
	public synchronized  boolean equals(Object obj) {

		return bf.equals(obj);
	}

	@Override
	public synchronized  String toString() {

		return bf.toString();
	}

}
