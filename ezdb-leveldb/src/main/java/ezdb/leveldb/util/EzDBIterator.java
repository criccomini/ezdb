package ezdb.leveldb.util;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

import org.iq80.leveldb.util.Slice;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface EzDBIterator extends Iterator<Map.Entry<Slice, Slice>>, Closeable {

	/**
	 * Repositions the iterator so the key of the next BlockElement returned greater
	 * than or equal to the specified targetKey.
	 */
	public void seek(Slice key);

	/**
	 * Repositions the iterator so is is at the beginning of the Database.
	 */
	public void seekToFirst();

	/**
	 * Returns the next element in the iteration, without advancing the iteration.
	 */
	public Map.Entry<Slice, Slice> peekNext();

	public Slice peekNextKey();

	/**
	 * @return true if there is a previous entry in the iteration.
	 */
	boolean hasPrev();

	/**
	 * @return the previous element in the iteration and rewinds the iteration.
	 */
	Map.Entry<Slice, Slice> prev();

	/**
	 * @return the previous element in the iteration, without rewinding the
	 *         iteration.
	 */
	public Map.Entry<Slice, Slice> peekPrev();

	public Slice peekPrevKey();

	/**
	 * Repositions the iterator so it is at the end of of the Database.
	 */
	public void seekToLast();

	@Override
	public void close();

}
