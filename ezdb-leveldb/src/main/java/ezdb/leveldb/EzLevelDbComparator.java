package ezdb.leveldb;

import java.util.Comparator;

import org.iq80.leveldb.DBComparator;

import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLevelDbComparator implements DBComparator {
	public static final String name = EzLevelDbComparator.class.toString();

	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public EzLevelDbComparator(final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final byte[] k1, final byte[] k2) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
	}

	@Override
	public byte[] findShortSuccessor(final byte[] key) {
		return key;
	}

	@Override
	public byte[] findShortestSeparator(final byte[] start, final byte[] limit) {
		return start;
	}

	@Override
	public String name() {
		return name;
	}
}
