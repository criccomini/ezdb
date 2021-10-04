package ezdb.lmdb.table;

import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.util.Util;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLmDbComparator implements Comparator<ByteBuffer> {

	private final Comparator<ByteBuffer> hashKeyComparator;

	public EzLmDbComparator(final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
	}

	@Override
	public int compare(final ByteBuffer a, final ByteBuffer b) {
		return Util.compareKeys(hashKeyComparator, a, b);
	}

}
