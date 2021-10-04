package ezdb.leveldb.util;

import java.io.Closeable;
import java.util.Iterator;

import org.iq80.leveldb.util.Slice;

import ezdb.table.RawTableRow;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface EzDBIterator<H, V> extends Iterator<RawTableRow<H, V>>, Closeable {

	/**
	 * Repositions the iterator so the key of the next BlockElement returned greater
	 * than or equal to the specified targetKey.
	 */
	void seek(Slice key);

	/**
	 * Repositions the iterator so is is at the beginning of the Database.
	 */
	void seekToFirst();

	/**
	 * Returns the next element in the iteration, without advancing the iteration.
	 */
	RawTableRow<H, V> peekNext();

	Slice peekNextKey();

	/**
	 * @return true if there is a previous entry in the iteration.
	 */
	boolean hasPrev();

	/**
	 * @return the previous element in the iteration and rewinds the iteration.
	 */
	RawTableRow<H, V> prev();

	Slice nextKey();

	/**
	 * @return the previous element in the iteration, without rewinding the
	 *         iteration.
	 */
	RawTableRow<H, V> peekPrev();

	Slice peekPrevKey();

	/**
	 * Repositions the iterator so it is at the end of of the Database.
	 */
	void seekToLast();

	@Override
	void close();

}
