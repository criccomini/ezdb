package ezdb.comparator;

import java.util.Comparator;

public class ComparableComparator implements Comparator<Object> {

	private static final ComparableComparator INSTANCE = new ComparableComparator();

	@SuppressWarnings("unchecked")
	public static <E> Comparator<E> get() {
		return (Comparator<E>) INSTANCE;
	}

	@Override
	public int compare(Object o1, Object o2) {
		boolean o1NullOrEmpty = o1 == null;
		boolean o2NullOrEmpty = o2 == null;
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
		Comparable<Object> co1 = toComparable(o1);
		Comparable<Object> co2 = toComparable(o2);
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
	protected Comparable<Object> toComparable(Object obj) {
		return (Comparable<Object>) obj;
	}

}
