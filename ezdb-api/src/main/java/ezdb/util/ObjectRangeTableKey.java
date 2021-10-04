package ezdb.util;

import java.util.Comparator;

public final class ObjectRangeTableKey<H, R> implements Comparable<ObjectRangeTableKey<H, R>> {
	private final H hashKey;
	private final R rangeKey;
	private final Comparator<ObjectRangeTableKey<H, R>> comparator;

	public ObjectRangeTableKey(final H hashKey, final R rangeKey, final Comparator<ObjectRangeTableKey<H, R>> comparator) {
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
	public int compareTo(final ObjectRangeTableKey<H, R> o) {
		return comparator.compare(this, o);
	}

}