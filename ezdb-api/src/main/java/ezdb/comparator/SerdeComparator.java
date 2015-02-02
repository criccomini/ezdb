package ezdb.comparator;

import java.util.Comparator;

import ezdb.serde.Serde;

public class SerdeComparator<O> implements Comparator<byte[]> {

	private Serde<O> serde;

	public SerdeComparator(Serde<O> serde) {
		this.serde = serde;
	}

	@Override
	public final int compare(byte[] o1, byte[] o2) {
		boolean o1NullOrEmpty = o1 == null || o1.length == 0;
		boolean o2NullOrEmpty = o2 == null || o2.length == 0;
		if (o1NullOrEmpty && o2NullOrEmpty) {
			return 0;
		}
		if (o1NullOrEmpty) {
			return -1;
		}
		if (o2NullOrEmpty) {
			// fix buffer underflow
			return 1;
		}
		Comparable<Object> co1 = toComparable(serde.fromBytes(o1));
		Comparable<Object> co2 = toComparable(serde.fromBytes(o2));
		return innerCompare(co1, co2);
	}

	/**
	 * Override this to customize the comparation itself. E.g. inversing it.
	 */
	protected int innerCompare(Comparable<Object> co1, Comparable<Object> co2) {
		return co1.compareTo(co2);
	}

	/**
	 * Override this to customize the comparable object. E.g. getting an inner
	 * object.
	 */
	@SuppressWarnings("unchecked")
	protected Comparable<Object> toComparable(Object fromBytes) {
		return (Comparable<Object>) fromBytes;
	}

}
