package ezdb.util;

import java.io.Closeable;
import java.util.Iterator;

import ezdb.table.TableRow;

public interface TableIterator<E extends TableRow<?, ?>> extends Iterator<E>, Closeable {
	/**
	 * Close the iterator and release all resources.
	 */
	@Override
	public void close();
}
