package ezdb.lsmtree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import com.indeed.util.serialization.Serializer;

import ezdb.serde.Serde;
import ezdb.util.ObjectTableKey;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class ObjectTableKeySerializer<H, R> implements Serializer<ObjectTableKey<H, R>> {

	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Comparator<ObjectTableKey<H, R>> comparator;

	public ObjectTableKeySerializer(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Comparator<ObjectTableKey<H, R>> comparator) {
		if (hashKeySerde == null) {
			throw new NullPointerException("hashKeySerde should not be null");
		}
		if (rangeKeySerde == null) {
			throw new NullPointerException("rangeKeySerde should not be null");
		}
		if (comparator == null) {
			throw new NullPointerException("comparator should not be null");
		}
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.comparator = comparator;
	}

	@Override
	public void write(final ObjectTableKey<H, R> t, final DataOutput out) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			Util.combineBuf(buffer, hashKeySerde, rangeKeySerde, t.getHashKey(), t.getRangeKey());
			EzdbSerializer.getBytesTo(buffer, out, buffer.readableBytes());
		} finally {
			buffer.release(buffer.refCnt());
		}

	}

	@Override
	public ObjectTableKey<H, R> read(final DataInput in) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			final int hashKeyBytesLength = in.readInt();
			EzdbSerializer.putBytesTo(buffer, in, hashKeyBytesLength);
			final H hashKey = hashKeySerde.fromBuffer(buffer);

			final int rangeKeyBytesLength = in.readInt();
			final R rangeKey;
			if (rangeKeyBytesLength > 0) {
				buffer.clear();
				EzdbSerializer.putBytesTo(buffer, in, rangeKeyBytesLength);
				rangeKey = rangeKeySerde.fromBuffer(buffer);
			} else {
				rangeKey = null;
			}
			return Util.combine(hashKey, rangeKey, comparator);
		} finally {
			buffer.release(buffer.refCnt());
		}

	}

}
