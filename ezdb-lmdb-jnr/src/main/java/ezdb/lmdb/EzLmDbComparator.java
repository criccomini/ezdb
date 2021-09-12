package ezdb.lmdb;

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
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzLmDbComparator(final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final ByteBuffer a, final ByteBuffer b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a, b);
	}

}
