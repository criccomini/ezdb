package ezdb.table;

import java.util.Map.Entry;

/**
 * A representation of a row in a table.
 * 
 * @author criccomini
 * 
 * @param <H> The type of the hash key for this key/value table.
 * @param <R> The type of the range key for this key/value table.
 * @param <V> The type of the value for this key/value table.
 */
public interface TableRow<H, V> extends Entry<H, V> {
	/**
	 * @return The hash key for this row.
	 */
	public H getHashKey();

	/**
	 * @return The value for this row.
	 */
	@Override
	public V getValue();

	@Override
	default H getKey() {
		return getHashKey();
	}

	@Override
	default V setValue(final V value) {
		throw new UnsupportedOperationException();
	}
}
