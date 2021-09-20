package ezdb.util;

import java.util.Comparator;

public final class ObjectTableKey<H, R> implements Comparable<ObjectTableKey<H, R>> {
	private final H hashKey;
	private final R rangeKey;
	private final Comparator<ObjectTableKey<H, R>> comparator;

	public ObjectTableKey(final H hashKey, final R rangeKey, final Comparator<ObjectTableKey<H, R>> comparator) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		this.comparator = comparator;
	}

	public H getHashKey() {
		return hashKey;
	}

	public R getRangeKey() {
		return rangeKey;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [hashKey=" + getHashKey() + ", rangeKey=" + getRangeKey() + "]";
	}

	@Override
	public int compareTo(final ObjectTableKey<H, R> o) {
		return comparator.compare(this, o);
	}

}