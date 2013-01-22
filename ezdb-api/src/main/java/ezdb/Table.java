package ezdb;

/**
 * A key/value table.
 * 
 * @author criccomini
 * 
 * @param <H>
 *          The type of the hash key for this key/value table.
 * @param <V>
 *          The type of the value for this key/value table.
 */
public interface Table<H, V> {
  /**
   * Put a value into the table, keyed by the hash key. If a row already exists
   * for the hash key, it should be overwritten.
   * 
   * @param hashKey
   *          The unique key associated with a value.
   * @param value
   *          The value to be persisted.
   */
  public void put(H hashKey, V value);

  /**
   * Get a value for a given key.
   * 
   * @param hashKey
   *          The unique key associated with a value.
   * @return The value, or null if no item matches the hash key.
   */
  public V get(H hashKey);

  /**
   * Delete a value for a given key. If no value exists for a given key, this
   * should be a no op.
   * 
   * @param hashKey
   *          The unique key associated with a value.
   */
  public void delete(H hashKey);

  /**
   * Close any open resources associated with this table.
   */
  public void close();
}
