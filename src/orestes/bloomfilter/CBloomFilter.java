package orestes.bloomfilter;

import java.util.BitSet;
import java.util.Collection;


public class CBloomFilter<T> extends BloomFilter<T> {
	protected BitSet counts;
	protected int c;
	protected OverflowHandler overflowHandler = new OverflowHandler() {

		@Override
		public void onOverflow() {
		}
	};

	/**
	 * Constructs a new counting bloom filter by determining the optimal bloom filter size <i>n</i> in bits and the
	 * number of hash functions <i>k</i> based on the expected number <i>n</i> of elements in the bloom filter and the
	 * tolerable false positive rate <i>p</i>.
	 * 
	 * @param n
	 *            Expected number of elements inserted in the bloom filter
	 * @param p
	 *            Tolerable false positive rate
	 * @param c
	 *            Number of bits used for counting
	 */
	public CBloomFilter(double n, double p, int c) {
		this(optimalM(n, p), optimalK(n, optimalM(n, p)), c);
	}

	/**
	 * Constructs an new Counting Bloom Filter. See constructor of superclass for details. Using c=4 is the best choice
	 * for most applications. Couting Bloom Filters behave like normal Bloom Filter but support a new operation:
	 * {@link #remove(Object)}. Elements can thus be added and removed from the bloom filter without loss of
	 * information.
	 * 
	 * @param m
	 *            The size of the bloom filter in bits.
	 * @param k
	 *            The number of hash functions to use.
	 * @param c
	 *            Number of bits used for counting
	 */
	public CBloomFilter(int m, int k, int c) {
		this(new BitSet(m * c), new BitSet(m), m, k, c);
	}

	protected CBloomFilter(BitSet counts, BitSet bloom, int m, int k, int c) {
		super(bloom, m, k);
		this.counts = counts;
		this.c = c;
	}

	/**
	 * Set an overflow hand which is called every time a counter has an overflow. This only happens if much more object
	 * are added to the couting bloom filter than it was configured for.
	 * 
	 * @param callback
	 *            the overflow callback to be executed
	 */
	public void setOverflowHandler(OverflowHandler callback) {
		this.overflowHandler = callback;
	}

	@Override
	public synchronized boolean add(byte[] value) {
		for (int position : hash(value)) {
			setBit(position);
			increment(position);
		}
			
		return true;
	}

	/**
	 * Removes the object from the counting bloom filter.
	 * 
	 * @param value
	 *            object to be deleted
	 */
	public synchronized void remove(byte[] value) {
		if (contains(value)) {
			for (int position : hash(value)) {
				decrement(position);
			}
		}
	}

	/**
	 * Removes the object from the counting bloom filter.
	 * 
	 * @param value
	 *            object to be deleted
	 */
	public synchronized void remove(T value) {
		remove(value.toString().getBytes(defaultCharset));
	}

	/**
	 * Removes the objects from the counting bloom filter.
	 * 
	 * @param values
	 *            objects to be deleted
	 */
	public synchronized void removeAll(Collection<T> values) {
		for (T value : values)
			remove(value);
	}

	/**
	 * Increment the iternal counter upon insertion of new elements.
	 * 
	 * @param index
	 *            position at which to increase
	 */
	protected void increment(int index) {
		int low = index * c;
		int high = (index + 1) * c;

		// Do a binary +1 on the slice of length c
		boolean incremented = false;
		for (int i = (high - 1); i >= low; i--) {
			if (!counts.get(i)) {
				counts.set(i);
				incremented = true;
				break;
			} else {
				counts.set(i, false);
			}
		}

		// If the counter overflowed, call the handler
		if (!incremented)
			overflowHandler.onOverflow();
	}

	/**
	 * Decrements the interal counter upon deletion.
	 * 
	 * @param index
	 *            position at which to decrease
	 */
	protected void decrement(int index) {
		int low = index * c;
		int high = (index + 1) * c;

		// Do a binary -1 on the counter's slice of length c
		boolean decremented = false;
		boolean nonZero = false;
		for (int i = (high - 1); i >= low; i--) {
			if (!decremented) {
				// Flip every bit until you reach the first bit that is one. Flip that and stop flipping.
				if (counts.get(i)) {
					counts.set(i, false);
					decremented = true;
				} else {
					counts.set(i, true);
					nonZero = true;
				}
			} else {
				// While there is still one bit that is not zero the counter isn't zero
				if (counts.get(i)) {
					nonZero = true;
				}
			}
		}

		// If the counter is down to zero, unset the bit in the bloom filter
		if (!nonZero)
			bloom.set(index, false);
	}

	/**
	 * Returns the number of bits used for counting
	 * 
	 * @param c
	 *            Number of bits used for counting
	 */
	public int getC() {
		return this.c;
	}

	@Override
	public synchronized boolean union(BloomFilter<T> other) {
		// FIXME
		return false;
	}

	@Override
	public synchronized boolean intersect(BloomFilter<T> other) {
		// FIXME
		return false;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Counting Bloom Filter, Parameters ");
		sb.append("m = " + getM() + ", ");
		sb.append("k = " + getK() + ", ");
		sb.append("c = " + getC() + "\n");
		for (int i = 0; i < m; i++) {
			sb.append(bloom.get(i) ? 1 : 0);
			sb.append(" ");
			if (counts != null) {
				for (int j = 0; j < c; j++) {
					sb.append(counts.get(c * i + j) ? 1 : 0);
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized Object clone() {
		CBloomFilter<T> o = (CBloomFilter<T>) super.clone();
		if(this.counts != null)
			o.counts = (BitSet) this.counts.clone();
		o.overflowHandler = this.overflowHandler;
		o.c = this.c;
		return o;
	}

	@Override
	public synchronized int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + c;
		result = prime * result + ((counts == null) ? 0 : counts.hashCode());
		result = prime * result + ((overflowHandler == null) ? 0 : overflowHandler.hashCode());
		return result;
	}

	@Override
	public synchronized boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CBloomFilter other = (CBloomFilter) obj;
		if (c != other.c)
			return false;
		if (counts == null) {
			if (other.counts != null)
				return false;
		} else if (!counts.equals(other.counts))
			return false;
		if (overflowHandler == null) {
			if (other.overflowHandler != null)
				return false;
		} else if (!overflowHandler.equals(other.overflowHandler))
			return false;
		return true;
	}

	@Override
	public synchronized void clear() {
		super.clear();
		counts.clear();
	}

	public static interface OverflowHandler {

		public void onOverflow();

	}
}
