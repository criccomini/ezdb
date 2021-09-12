package ezdb;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator for table rows.
 * 
 * @author criccomini
 * 
 * @param <H> The type of the hash key for this key/value table.
 * @param <R> The type of the range key for this key/value table.
 * @param <V> The type of the value for this key/value table.
 */
public interface TableIterator<H, R, V> extends Iterator<TableRow<H, R, V>>, Closeable {
	/**
	 * Close the iterator and release all resources.
	 */
	@Override
	public void close();
}
