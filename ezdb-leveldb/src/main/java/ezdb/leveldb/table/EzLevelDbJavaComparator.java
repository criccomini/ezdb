package ezdb.leveldb.table;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.iq80.leveldb.util.Slice;

import ezdb.leveldb.util.Slices;
import ezdb.leveldb.util.ZeroCopyDBComparator;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLevelDbJavaComparator implements ZeroCopyDBComparator {
	public static final String name = EzLevelDbJavaComparator.class.toString();

	private final Comparator<ByteBuffer> hashKeyComparator;

	public EzLevelDbJavaComparator(final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
	}

	/**
	 * LevelDBJni still uses this slow version
	 */
	@Deprecated
	@Override
	public int compare(final byte[] k1, final byte[] k2) {
		throw new UnsupportedOperationException("should use zero copy method");
	}

	public int compare(final Slice k1, final Slice k2) {
		return Slices.compareKeys(hashKeyComparator, k1, k2);
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
