package ezdb.util;

public final class ObjectTableKey<H, R> {
	private final H hashKey;
	private final R rangeKey;

	public ObjectTableKey(H hashKey, R rangeKey) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
	}

	public H getHashKey() {
		return hashKey;
	}

	public R getRangeKey() {
		return rangeKey;
	}
	
}