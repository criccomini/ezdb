package ezdb.comparator;

import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.serde.Serde;

public class SerdeComparator<O> implements Comparator<ByteBuffer> {

	private final Serde<O> serde;

	public SerdeComparator(final Serde<O> serde) {
		this.serde = serde;
	}

	@Override
	public final int compare(final ByteBuffer o1, final ByteBuffer o2) {
		final boolean o1NullOrEmpty = o1 == null || o1.remaining() == 0;
		final boolean o2NullOrEmpty = o2 == null || o2.remaining() == 0;
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
		final Comparable<Object> co1 = toComparable(serde.fromBuffer(o1));
		final Comparable<Object> co2 = toComparable(serde.fromBuffer(o2));
		return innerCompare(co1, co2);
	}

	/**
	 * Override this to customize the comparation itself. E.g. inversing it.
	 */
	protected int innerCompare(final Comparable<Object> co1, final Comparable<Object> co2) {
		return co1.compareTo(co2);
	}

	/**
	 * Override this to customize the comparable object. E.g. getting an inner
	 * object.
	 */
	@SuppressWarnings("unchecked")
	protected Comparable<Object> toComparable(final Object fromBytes) {
		return (Comparable<Object>) fromBytes;
	}

}
