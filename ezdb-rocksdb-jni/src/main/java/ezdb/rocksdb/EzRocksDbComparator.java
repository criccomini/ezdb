package ezdb.rocksdb;

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

	private final Comparator<byte[]> hashKeyComparator;
	private final Comparator<byte[]> rangeKeyComparator;

	public EzRocksDbComparator(Comparator<byte[]> hashKeyComparator,
			Comparator<byte[]> rangeKeyComparator) {
		super(new ComparatorOptions());
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(Slice a, Slice b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a.data(), b.data());
	}

	@Override
	public String name() {
		return name;
	}
}
