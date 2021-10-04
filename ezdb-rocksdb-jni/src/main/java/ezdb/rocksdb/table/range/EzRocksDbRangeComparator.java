package ezdb.rocksdb.table.range;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.Slice;

import ezdb.util.Util;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzRocksDbRangeComparator extends org.rocksdb.Comparator {
	public static final String name = EzRocksDbRangeComparator.class.toString();

	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzRocksDbRangeComparator(final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		super(new ComparatorOptions());
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final Slice a, final Slice b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, ByteBuffer.wrap(a.data()),
				ByteBuffer.wrap(b.data()));
	}

	@Override
	public String name() {
		return name;
	}
}
