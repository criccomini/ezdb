package ezdb.lsmtree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.indeed.util.serialization.Serializer;

import ezdb.serde.Serde;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

@Immutable
public final class EzdbSerializer<E> implements Serializer<E> {

	private final Serde<E> serde;

	private EzdbSerializer(final Serde<E> serde) {
		this.serde = serde;
	}

	@Override
	public void write(final E t, final DataOutput out) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			serde.toBuffer(buffer, t);
			final int length = buffer.readableBytes();
			out.writeInt(length);
			getBytesTo(buffer, out, length);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public E read(final DataInput in) throws IOException {
		final ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer();
		try {
			final int length = in.readInt();
			ensureCapacity(buffer, length);
			putBytesTo(buffer, in, length);
			return serde.fromBuffer(buffer);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	private static void getBytesTo(final ByteBuf buffer, final DataOutput dst, final int length) throws IOException {
		int i = 0;
		while (i < length) {
			final byte b = buffer.getByte(i);
			dst.write(b);
			i++;
		}
	}

	private static void putBytesTo(final ByteBuf buffer, final DataInput src, final int length) throws IOException {
		ensureCapacity(buffer, length);
		int i = 0;
		while (i < length) {
			final byte b = src.readByte();
			buffer.writeByte(b);
			i++;
		}
	}

	private static void ensureCapacity(final ByteBuf buffer, final int desiredCapacity) {
		if (buffer.capacity() < desiredCapacity) {
			buffer.capacity(desiredCapacity);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Serializer<T> valueOf(final Serde<T> delegate) {
		if (delegate == null) {
			return null;
		}
		final Serializer<T> unwrapped = delegate.unwrap(Serializer.class);
		if (unwrapped != null) {
			return unwrapped;
		} else if (delegate instanceof Serializer) {
			return (Serializer<T>) delegate;
		} else {
			return new EzdbSerializer<T>(delegate);
		}
	}
}