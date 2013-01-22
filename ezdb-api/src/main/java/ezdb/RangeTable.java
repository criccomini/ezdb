package ezdb;

/**
 * A hash/range table that supports bucketing rows together by hash key, and
 * doing range queries within the buckets. Rows are uniquely identified by
 * combining their hash key and range key.
 * 
 * @author criccomini
 * 
 * @param <H>
 *          The type of the hash key for this key/value table.
 * @param <R>
 *          The type of the range key for this key/value table.
 * @param <V>
 *          The type of the value for this key/value table.
 */
public interface RangeTable<H, R, V> extends Table<H, V> {
  /**
   * Put a value into the table, keyed by both the hash and range key. If a row
   * already exists for the hash/range pair, it should be overwritten.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @param rangeKey
   *          A secondary key used to sort rows within the same hash key group.
   * @param value
   *          The value to be persisted.
   */
  public void put(H hashKey, R rangeKey, V value);

  /**
   * Get a value for a given hash/range pair.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @param rangeKey
   *          A secondary key used to sort rows within the same hash key group.
   * @return The value, or null if the item matches the hash/range pair.
   */
  public V get(H hashKey, R rangeKey);

  /**
   * Get all rows with a given hash key.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @return An iterator of all TableRows in this hash key group, ordered by
   *         their range key.
   */
  public TableIterator<H, R, V> range(H hashKey);

  /**
   * Get all rows with a given hash key, and a range key that's greater than or
   * equal to the "from" range key provided.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @param fromRangeKey
   *          The range key to use as the starting point. If an exact match
   *          doesn't exist for this hash/range pair, the nearest range key that
   *          is larger than this range key will be used as the starting point.
   * @return An iterator of all remaining TableRows in this hash key group,
   *         ordered by their range key.
   */
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey);

  /**
   * Get all rows with a given hash key, and a range key that's greater than or
   * equal to the "from" range key, and less than or equal to the "to" range
   * key. That is, the "from" range key is inclusive, and the "to" range key is
   * exclusive.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @param fromRangeKey
   *          The range key to use as the starting point. If an exact match
   *          doesn't exist for this hash/range pair, the nearest range key that
   *          is larger than this range key will be used as the starting point.
   * @param toRangeKey
   *          The range key to use as the end point (exclusive). If an exact
   *          match doesn't exist, the nearest range key that is less than this
   *          range key will be used as the end point (inclusive).
   * @return An iterator of all remaining TableRows in this hash key group that
   *         have a range key [fromRangeKey, toRangeKey).
   */
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey);

  /**
   * Delete a value for a given hash/range pair. If no value exists for a given
   * hash/range pair, this should be a no op.
   * 
   * @param hashKey
   *          A key used group rows together.
   * @param rangeKey
   *          A secondary key used to sort rows within the same hash key group.
   */
  public void delete(H hashKey, R rangeKey);
}
