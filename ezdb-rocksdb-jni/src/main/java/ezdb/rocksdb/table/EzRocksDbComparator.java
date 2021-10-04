package ezdb.rocksdb.table;

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
public class EzRocksDbComparator extends org.rocksdb.Comparator {
	public static final String name = EzRocksDbComparator.class.toString();

	private final Comparator<ByteBuffer> hashKeyComparator;

	public EzRocksDbComparator(final Comparator<ByteBuffer> hashKeyComparator) {
		super(new ComparatorOptions());
		this.hashKeyComparator = hashKeyComparator;
	}

	@Override
	public int compare(final Slice a, final Slice b) {
		return Util.compareKeys(hashKeyComparator, ByteBuffer.wrap(a.data()), ByteBuffer.wrap(b.data()));
	}

	@Override
	public String name() {
		return name;
	}
}
