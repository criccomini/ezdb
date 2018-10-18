package ezdb.mdbx;

import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.mdbx.util.DirectBuffers;
import ezdb.util.Util;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzMdbxDbComparator implements Comparator<ByteBuffer> {

	private final Comparator<byte[]> hashKeyComparator;
	private final Comparator<byte[]> rangeKeyComparator;

	public EzMdbxDbComparator(Comparator<byte[]> hashKeyComparator,
			Comparator<byte[]> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}
	
	@Override
	public int compare(ByteBuffer a, ByteBuffer b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, DirectBuffers.array(a), DirectBuffers.array(b));
	}

}
