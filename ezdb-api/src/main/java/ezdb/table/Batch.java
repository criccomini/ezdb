package ezdb.table;

import java.io.Closeable;
import java.io.IOException;

import ezdb.util.Util;

public interface Batch<H, V> extends Closeable {

	/**
	   * Put a value into the table, keyed by the hash key. If a row already exists
	   * for the hash key, it should be overwritten.
	   * 
	   * @param hashKey
	   *          The unique key associated with a value.
	   * @param value
	   *          The value to be persisted.
	   */
	void put(H hashKey, V value);
	
	 /**
	   * Delete a value for a given key. If no value exists for a given key, this
	   * should be a no op.
	   * 
	   * @param hashKey
	   *          The unique key associated with a value.
	   */
	void delete(H hashKey);

	/**
	 * You need to call flush() before close() in order not to loose your data.
	 */
	void flush();
	
}
