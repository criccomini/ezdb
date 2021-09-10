package ezdb.rocksdb;

import java.util.Comparator;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.Slice;

import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzRocksDbComparator extends org.rocksdb.Comparator {
	public static final String name = EzRocksDbComparator.class.toString();

	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public EzRocksDbComparator(final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator) {
		super(new ComparatorOptions());
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final Slice a, final Slice b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a.data(), b.data());
	}

	@Override
	public String name() {
		return name;
	}
}
