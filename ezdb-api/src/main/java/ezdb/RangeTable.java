package ezdb;

import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;

/**
 * A hash/range table that supports bucketing rows together by hash key, and
 * doing range queries within the buckets. Rows are uniquely identified by
 * combining their hash key and range key.
 * 
 * @author criccomini
 * 
 * @param <H>
 *            The type of the hash key for this key/value table.
 * @param <R>
 *            The type of the range key for this key/value table.
 * @param <V>
 *            The type of the value for this key/value table.
 */
public interface RangeTable<H, R, V> extends Table<H, V> {
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
	public void put(H hashKey, R rangeKey, V value);

	/**
	 * Get a value for a given hash/range pair.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 * @return The value, or null if the item does not match the hash/range
	 *         pair.
	 */
	public V get(H hashKey, R rangeKey);

	/**
	 * Get the latest value for a given hash/range pair. It gets the overall
	 * latest.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @return The latest value, or null if the item does not match the hash.
	 */
	public TableRow<H, R, V> getLatest(final H hashKey);

	/**
	 * Get the latest value for a given hash/range pair. First it tries to get
	 * the value that is &lt;= rangeKey, if that fails it returns the value that
	 * is &gt;= rangeKey. If rangeKey is null, it gets the overall latest.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 * @return The latest value, or null if the item does not match the
	 *         hash/range pair.
	 */
	public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey);

	/**
	 * Get the next value for a given hash/range pair. That is the value that is
	 * &gt;= rangeKey. If rangeKey is null, this returns the first value
	 * overall.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 * @return The next value, or null if the item does not match the hash/range
	 *         pair.
	 */
	public TableRow<H, R, V> getNext(final H hashKey, final R rangeKey);

	/**
	 * Get the previous value for a given hash/range pair. That is the value
	 * that is &lt;= rangeKey. If rangeKey is null, this returns the last value
	 * overall.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param rangeKey
	 *            A secondary key used to sort rows within the same hash key
	 *            group.
	 * @return The previous value, or null if the item does not match the
	 *         hash/range pair.
	 */
	public TableRow<H, R, V> getPrev(final H hashKey, final R rangeKey);

	/**
	 * Get all rows with a given hash key.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @return An iterator of all TableRows in this hash key group, ordered by
	 *         their range key.
	 */
	public TableIterator<H, R, V> range(H hashKey);

	/**
	 * Get all rows with a given hash key, and a range key that's greater than
	 * or equal to the "from" range key provided.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param fromRangeKey
	 *            The range key to use as the starting point. If an exact match
	 *            doesn't exist for this hash/range pair, the nearest range key
	 *            that is larger than this range key will be used as the
	 *            starting point.
	 * @return An iterator of all remaining TableRows in this hash key group,
	 *         ordered by their range key.
	 */
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey);

	/**
	 * Get all rows with a given hash key, and a range key that's greater than
	 * or equal to the "from" range key, and less than or equal to the "to"
	 * range key. That is, the "from" range key is inclusive, and the "to" range
	 * key is inclusive.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param fromRangeKey
	 *            The range key to use as the starting point. If an exact match
	 *            doesn't exist for this hash/range pair, the nearest range key
	 *            that is larger than this range key will be used as the
	 *            starting point.
	 * @param toRangeKey
	 *            The range key to use as the end point (inclusive). If an exact
	 *            match doesn't exist, the nearest range key that is less than
	 *            this range key will be used as the end point (inclusive).
	 * @return An iterator of all remaining TableRows in this hash key group
	 *         that have a range key [fromRangeKey, toRangeKey).
	 */
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey);

	/**
	 * Get all rows with a given hash key in reverse iteration order.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @return An iterator of all TableRows in this hash key group, ordered by
	 *         their range key.
	 */
	public TableIterator<H, R, V> rangeReverse(H hashKey);

	/**
	 * Get all rows with a given hash key, and a range key that's smaller than
	 * or equal to the "from" range key provided in reverse iteration order.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param fromRangeKey
	 *            The range key to use as the starting point. If an exact match
	 *            doesn't exist for this hash/range pair, the nearest range key
	 *            that is less than this range key will be used as the starting
	 *            point.
	 * @return An iterator of all remaining TableRows in this hash key group,
	 *         reverse ordered by their range key.
	 */
	public TableIterator<H, R, V> rangeReverse(H hashKey, R fromRangeKey);

	/**
	 * Get all rows with a given hash key, and a range key that's smaller than
	 * or equal to the "from" range key, and greater than or equal to the "to"
	 * range key in reverse iteration order. That is, the "from" range key is
	 * inclusive, and the "to" range key is inclusive.
	 * 
	 * @param hashKey
	 *            A key used group rows together.
	 * @param fromRangeKey
	 *            The range key to use as the starting point. If an exact match
	 *            doesn't exist for this hash/range pair, the nearest range key
	 *            that is less than this range key will be used as the starting
	 *            point.
	 * @param toRangeKey
	 *            The range key to use as the end point (inclusive). If an exact
	 *            match doesn't exist, the nearest range key that is larger than
	 *            this range key will be used as the end point (inclusive).
	 * @return An iterator in reverse order of all remaining TableRows in this
	 *         hash key group that have a range key [fromRangeKey, toRangeKey).
	 */
	public TableIterator<H, R, V> rangeReverse(H hashKey, R fromRangeKey,
			R toRangeKey);

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
	public void delete(H hashKey, R rangeKey);

	/**
	 * With this it is possible to do bulk/batch puts and deletes.
	 * 
	 * @return a new batch enabled transaction object
	 */
	public RangeBatch<H, R, V> newRangeBatch();
	
	/**
	 * Compacts the given range, if all keys are null, everything is compacted.
	 * 
	 * @param fromHashKey
	 *            A key used group rows together.
	 * @param fromRangeKey
	 *            The range key to use as the starting point. If an exact match
	 *            doesn't exist for this hash/range pair, the nearest range key
	 *            that is less than this range key will be used as the starting
	 *            point.
	 * @param toHashKey
	 *            A key used group rows together.
	 * @param toRangeKey
	 *            The range key to use as the end point (inclusive). If an exact
	 *            match doesn't exist, the nearest range key that is larger than
	 *            this range key will be used as the end point (inclusive).
	 */
	public void compactRange(H fromHashKey, R fromRangeKey, H toHashKey, R toRangeKey);

}
