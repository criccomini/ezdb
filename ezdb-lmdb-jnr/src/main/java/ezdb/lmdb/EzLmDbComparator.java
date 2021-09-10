package ezdb.lmdb;

import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLmDbComparator implements Comparator<ByteBuffer> {

	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public EzLmDbComparator(final Comparator<ByteBuf> hashKeyComparator, final Comparator<ByteBuf> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final ByteBuffer a, final ByteBuffer b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, Unpooled.wrappedBuffer(a),
				Unpooled.wrappedBuffer(b));
	}

}
