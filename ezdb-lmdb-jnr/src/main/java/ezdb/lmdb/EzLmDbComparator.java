package ezdb.lmdb;

import java.util.Comparator;

import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLmDbComparator implements Comparator<ByteBuf> {

	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public EzLmDbComparator(final Comparator<ByteBuf> hashKeyComparator, final Comparator<ByteBuf> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final ByteBuf a, final ByteBuf b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a, b);
	}

}
