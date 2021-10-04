package ezdb.lmdb.util;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;

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
	public void seek(ByteBuffer key);

	/**
	 * Repositions the iterator so is is at the beginning of the Database.
	 */
	public void seekToFirst();

	/**
	 * Returns the next element in the iteration, without advancing the iteration.
	 */
	public RawTableRow<H, V> peekNext();

	public ByteBuffer nextKey();

	/**
	 * @return true if there is a previous entry in the iteration.
	 */
	boolean hasPrev();

	/**
	 * @return the previous element in the iteration and rewinds the iteration.
	 */
	RawTableRow<H, V> prev();

	/**
	 * @return the previous element in the iteration, without rewinding the
	 *         iteration.
	 */
	public RawTableRow<H, V> peekPrev();

	/**
	 * Repositions the iterator so it is at the end of of the Database.
	 */
	public void seekToLast();

	@Override
	public void close();

	public ByteBuffer peekNextKey();

	public ByteBuffer peekPrevKey();

}
