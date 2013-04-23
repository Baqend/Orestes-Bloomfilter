package orestes.bloomfilter;

import java.util.BitSet;
import java.util.Collection;
import java.util.Random;
import java.util.zip.Checksum;

/**
 * * A wrapper for a Counting Bloom filter that synchronizes access for
 * concurrent access similar to Collections.synchronizedCollection.
 * 
 * @param <T>
 *            Type Parameter of the Counting Bloom filter
 */
public class SynchronizedCBloomFilter<T> extends CBloomFilter<T> {
	private CBloomFilter<T> cbf;

	public SynchronizedCBloomFilter(CBloomFilter<T> cbf) {
		this.cbf = cbf;
	}

	public synchronized void setOverflowHandler(
			orestes.bloomfilter.CBloomFilter.OverflowHandler callback) {
		cbf.setOverflowHandler(callback);
	}

	public synchronized boolean add(byte[] value) {
		return cbf.add(value);
	}

	public synchronized void remove(byte[] value) {
		cbf.remove(value);
	}

	public synchronized void remove(T value) {
		cbf.remove(value);
	}

	public synchronized void removeAll(Collection<T> values) {
		cbf.removeAll(values);
	}

	public synchronized void setHashMethod(
			orestes.bloomfilter.BloomFilter.HashMethod hashMethod) {
		cbf.setHashMethod(hashMethod);
	}

	public synchronized int getC() {
		return cbf.getC();
	}

	public synchronized boolean union(BloomFilter<T> other) {
		return cbf.union(other);
	}

	public synchronized boolean intersect(BloomFilter<T> other) {
		return cbf.intersect(other);
	}

	public synchronized String toString() {
		return cbf.toString();
	}

	public synchronized Object clone() {
		return cbf.clone();
	}

	public synchronized int hashCode() {
		return cbf.hashCode();
	}

	public synchronized void setCryptographicHashFunction(
			String hashFunctionName) {
		cbf.setCryptographicHashFunction(hashFunctionName);
	}

	public synchronized boolean equals(Object obj) {
		return cbf.equals(obj);
	}

	public synchronized void setCusomHashFunction(
			orestes.bloomfilter.BloomFilter.CustomHashFunction chf) {
		cbf.setCusomHashFunction(chf);
	}

	public synchronized void clear() {
		cbf.clear();
	}

	public synchronized boolean add(T value) {
		return cbf.add(value);
	}

	public synchronized void addAll(Collection<T> values) {
		cbf.addAll(values);
	}

	public synchronized boolean contains(byte[] value) {
		return cbf.contains(value);
	}

	public synchronized boolean contains(T value) {
		return cbf.contains(value);
	}

	public synchronized boolean containsAll(Collection<T> values) {
		return cbf.containsAll(values);
	}

	public synchronized BitSet getBitSet() {
		return cbf.getBitSet();
	}

	public synchronized int[] hash(String value) {
		return cbf.hash(value);
	}

	public synchronized boolean isEmpty() {
		return cbf.isEmpty();
	}

	public synchronized double getFalsePositiveProbability(int n) {
		return cbf.getFalsePositiveProbability(n);
	}

	public synchronized double getBitsPerElement(int n) {
		return cbf.getBitsPerElement(n);
	}

	public synchronized double getBitZeroProbability(int n) {
		return cbf.getBitZeroProbability(n);
	}

	public synchronized int size() {
		return cbf.size();
	}

	public synchronized int getM() {
		return cbf.getM();
	}

	public synchronized int getK() {
		return cbf.getK();
	}

	public synchronized String getCryptographicHashFunctionName() {
		return cbf.getCryptographicHashFunctionName();
	}

	public synchronized orestes.bloomfilter.BloomFilter.HashMethod getHashMethod() {
		return cbf.getHashMethod();
	}

	@Override
	protected synchronized void increment(int index) {

		cbf.increment(index);
	}

	@Override
	protected synchronized void decrement(int index) {

		cbf.decrement(index);
	}

	@Override
	protected synchronized boolean getBit(int index) {

		return cbf.getBit(index);
	}

	@Override
	protected synchronized void setBit(int index) {

		cbf.setBit(index);
	}

	@Override
	protected synchronized void setBit(int index, boolean to) {

		cbf.setBit(index, to);
	}

	@Override
	protected synchronized int[] hashCarterWegman(byte[] value) {

		return cbf.hashCarterWegman(value);
	}

	@Override
	protected synchronized int[] hashRNG(byte[] value) {

		return cbf.hashRNG(value);
	}

	@Override
	protected synchronized int[] hashSecureRNG(byte[] value) {

		return cbf.hashSecureRNG(value);
	}

	@Override
	protected synchronized int[] hashMagnus(byte[] value) {

		return cbf.hashMagnus(value);
	}

	@Override
	protected synchronized int[] hashCRC(byte[] value) {

		return cbf.hashCRC(value);
	}

	@Override
	protected synchronized int[] hashAdler(byte[] value) {

		return cbf.hashAdler(value);
	}

	@Override
	protected synchronized int[] hashChecksum(byte[] value, Checksum cs) {

		return cbf.hashChecksum(value, cs);
	}

	@Override
	protected synchronized int[] hashLCG(byte[] value) {
		return cbf.hashLCG(value);
	}

	@Override
	protected synchronized int[] hashMurmur(byte[] value) {
		return cbf.hashMurmur(value);
	}

	@Override
	protected synchronized int rejectionSample(int random) {
		return cbf.rejectionSample(random);
	}

	@Override
	protected synchronized int[] hashCrypt(byte[] value) {
		return cbf.hashCrypt(value);
	}

	@Override
	protected synchronized int[] hash(byte[] value) {
		return cbf.hash(value);
	}

	@Override
	protected synchronized boolean compatible(BloomFilter<T> bloomFilter,
			BloomFilter<T> other) {
		return cbf.compatible(bloomFilter, other);
	}

	@Override
	protected synchronized Random getRandom() {
		return cbf.getRandom();
	}

}
