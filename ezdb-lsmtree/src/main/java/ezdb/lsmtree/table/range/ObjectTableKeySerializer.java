package ezdb.lsmtree.table.range;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import com.indeed.util.serialization.Serializer;

import ezdb.lsmtree.EzdbSerializer;
import ezdb.serde.Serde;
import ezdb.util.ObjectRangeTableKey;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class ObjectTableKeySerializer<H, R> implements Serializer<ObjectRangeTableKey<H, R>> {

	private final EzdbSerializer<H> hashKeySerializer;
	private final EzdbSerializer<R> rangeKeySerializer;
	private final Comparator<ObjectRangeTableKey<H, R>> comparator;

	public ObjectTableKeySerializer(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Comparator<ObjectRangeTableKey<H, R>> comparator) {
		if (hashKeySerde == null) {
			throw new NullPointerException("hashKeySerde should not be null");
		}
		if (rangeKeySerde == null) {
			throw new NullPointerException("rangeKeySerde should not be null");
		}
		if (comparator == null) {
			throw new NullPointerException("comparator should not be null");
		}
		this.hashKeySerializer = new EzdbSerializer<H>(hashKeySerde);
		this.rangeKeySerializer = new EzdbSerializer<R>(rangeKeySerde);
		this.comparator = comparator;
	}

	@Override
	public void write(final ObjectRangeTableKey<H, R> t, final DataOutput out) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			hashKeySerializer.writeWithBuffer(buffer, t.getHashKey(), out);
			buffer.clear();
			rangeKeySerializer.writeWithBuffer(buffer, t.getRangeKey(), out);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public ObjectRangeTableKey<H, R> read(final DataInput in) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			final H hashKey = hashKeySerializer.readWithBuffer(buffer, in);
			buffer.clear();
			final R rangeKey = rangeKeySerializer.readWithBuffer(buffer, in);
			return Util.combine(hashKey, rangeKey, comparator);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

}
