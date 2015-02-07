package ezdb.batch;

import java.io.Closeable;
import java.io.IOException;

import ezdb.util.Util;

public interface RangeBatch<H, R, V> extends Batch<H, V> {

	/**
	 * Put a value into the table, keyed by both the hash and range key. If a
	 * row already exists for the hash/range pair, it should be overwritten.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 * @param value
	 *            The value to be persisted.
	 */
	void put(H hashKey, R rangeKey, V value);

	/**
	 * Delete a value for a given hash/range pair. If no value exists for a
	 * given hash/range pair, this should be a no op.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 */
	void delete(H hashKey, R rangeKey);

}
