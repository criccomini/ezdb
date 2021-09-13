package ezdb.rocksdb.util;

import java.io.Closeable;
import java.util.Iterator;

import ezdb.RawTableRow;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface EzDBIterator<H, R, V> extends Iterator<RawTableRow<H, R, V>>, Closeable {

	/**
	 * Repositions the iterator so the key of the next BlockElement returned greater
	 * than or equal to the specified targetKey.
	 */
	public void seek(byte[] key);

	/**
	 * Repositions the iterator so is is at the beginning of the Database.
	 */
	public void seekToFirst();

	/**
	 * Returns the next element in the iteration, without advancing the iteration.
	 */
	public RawTableRow<H, R, V> peekNext();

	public byte[] nextKey();

	public byte[] peekNextKey();

	/**
	 * @return true if there is a previous entry in the iteration.
	 */
	boolean hasPrev();

	/**
	 * @return the previous element in the iteration and rewinds the iteration.
	 */
	RawTableRow<H, R, V> prev();

	/**
	 * @return the previous element in the iteration, without rewinding the
	 *         iteration.
	 */
	RawTableRow<H, R, V> peekPrev();

	public byte[] peekPrevKey();

	/**
	 * Repositions the iterator so it is at the end of of the Database.
	 */
	public void seekToLast();

	@Override
	public void close();

}
