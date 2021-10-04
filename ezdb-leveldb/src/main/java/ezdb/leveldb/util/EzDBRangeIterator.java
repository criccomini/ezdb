package ezdb.leveldb.util;

import java.io.Closeable;
import java.util.Iterator;

import org.iq80.leveldb.util.Slice;

import ezdb.table.range.RawRangeTableRow;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface EzDBRangeIterator<H, R, V> extends Iterator<RawRangeTableRow<H, R, V>>, Closeable {

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
	RawRangeTableRow<H, R, V> peekNext();

	Slice peekNextKey();

	/**
	 * @return true if there is a previous entry in the iteration.
	 */
	boolean hasPrev();

	/**
	 * @return the previous element in the iteration and rewinds the iteration.
	 */
	RawRangeTableRow<H, R, V> prev();

	Slice nextKey();

	/**
	 * @return the previous element in the iteration, without rewinding the
	 *         iteration.
	 */
	RawRangeTableRow<H, R, V> peekPrev();

	Slice peekPrevKey();

	/**
	 * Repositions the iterator so it is at the end of of the Database.
	 */
	void seekToLast();

	@Override
	void close();

}
