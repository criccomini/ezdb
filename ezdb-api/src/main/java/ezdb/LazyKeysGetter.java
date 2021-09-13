package ezdb;

//@NotThreadSafe
public abstract class LazyKeysGetter<K, R> {

	private boolean initialized;
	protected K hashKey;
	protected R rangeKey;

	private void maybeInitialize() {
		if (!initialized) {
			initialize();
		}
	}

	protected abstract void initialize();

	public final K getHashKey() {
		maybeInitialize();
		return hashKey;
	}

	public final R getRangeKey() {
		maybeInitialize();
		return rangeKey;
	}

}
