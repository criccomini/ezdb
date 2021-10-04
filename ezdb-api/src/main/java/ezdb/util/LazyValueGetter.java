package ezdb.util;

import java.util.function.Supplier;

//@NotThreadSafe
public abstract class LazyValueGetter<T> implements Supplier<T> {

	private boolean initialized;
	private T value;

	@Override
	public final T get() {
		if (!initialized) {
			value = initialize();
			initialized = true;
		}
		return value;
	}

	protected abstract T initialize();

}
