package ezdb.lmdb;

import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.lmdb.util.DirectBuffers;
import ezdb.util.Util;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLmDbComparator implements Comparator<ByteBuffer> {

	private final Comparator<byte[]> hashKeyComparator;
	private final Comparator<byte[]> rangeKeyComparator;

	public EzLmDbComparator(Comparator<byte[]> hashKeyComparator,
			Comparator<byte[]> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}
	
	@Override
	public int compare(ByteBuffer a, ByteBuffer b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, DirectBuffers.array(a), DirectBuffers.array(b));
	}

}
