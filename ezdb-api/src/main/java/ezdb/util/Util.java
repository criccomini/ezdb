package ezdb.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Comparator;

import ezdb.serde.Serde;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class Util {

	private static final ISliceInvoker SLICE_INVOKER;
	public static final byte[] EMPTY_BYTES = new byte[0];

	static {
		SLICE_INVOKER = newSliceInvoker();
	}

	private static ISliceInvoker newSliceInvoker() {
		try {
			// java >= 13
			final Method sliceMethod = java.nio.ByteBuffer.class.getDeclaredMethod("slice", int.class, int.class);
			final MethodHandle sliceInvoker = MethodHandles.lookup().unreflect(sliceMethod);
			return (buffer, position, length) -> {
				try {
					return (java.nio.ByteBuffer) sliceInvoker.invoke(buffer, position, length);
				} catch (final Throwable e) {
					throw new RuntimeException(e);
				}
			};
		} catch (final Throwable e) {
			// java < 13
			return (buffer, position, length) -> {
				final java.nio.ByteBuffer duplicate = buffer.duplicate();
				position(duplicate, position);
				duplicate.limit(position + length);
				return duplicate.slice();
			};
		}
	}

	@FunctionalInterface
	private interface ISliceInvoker {
		java.nio.ByteBuffer slice(java.nio.ByteBuffer buffer, int position, int length);
	}

	/**
	 * Workaround for java 8 compiled on java 9 or higher
	 */
	public static void position(final Buffer buffer, final int position) {
		buffer.position(position);
	}

	public static java.nio.ByteBuffer slice(final java.nio.ByteBuffer buffer, final int position, final int length) {
		return SLICE_INVOKER.slice(buffer, position, length);
	}

	public static <H, R> void combineBuf(final ByteBuf buffer, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final H hashKey, final R rangeKey) {
		combineBuf(buffer, hashKeySerde, hashKey);
		combineBuf(buffer, rangeKeySerde, rangeKey);
	}

	public static <V> void combineBuf(final ByteBuf buffer, final Serde<V> serde, final V value) {
		if (value == null) {
			buffer.writeInt(0);
			return;
		}
		final int writeIndexBefore = buffer.writerIndex();
		buffer.writerIndex(writeIndexBefore + Integer.BYTES);
		serde.toBuffer(buffer, value);
		final int length = buffer.writerIndex() - writeIndexBefore - Integer.BYTES;
		buffer.setInt(writeIndexBefore, length);
	}

	public static <H, R> ByteBuffer combineBuffer(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final H hashKey, final R rangeKey) {
		final ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
		combineBuf(buf, hashKeySerde, hashKey);
		combineBuf(buf, rangeKeySerde, rangeKey);

		final ByteBuffer buffer = toByteBuffer(buf);
		buf.release(buf.refCnt());
		buffer.clear();
		return buffer;
	}

	public static ByteBuffer toByteBuffer(final ByteBuf buf) {
		final ByteBuffer buffer = ByteBuffer.allocate(buf.readableBytes());
		buf.readBytes(buffer);
		return buffer;
	}

	public static <H, R> byte[] combineBytes(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final H hashKey,
			final R rangeKey) {
		final ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
		combineBuf(buf, hashKeySerde, hashKey);
		combineBuf(buf, rangeKeySerde, rangeKey);

		final byte[] bytes = toByteArray(buf);
		buf.release(buf.refCnt());
		return bytes;
	}

	public static <H, R> byte[] combineBytes(final Serde<H> hashKeySerde, final H hashKey) {
		return hashKeySerde.toBytes(hashKey);
	}

	public static int combinedSize(final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		return Integer.BYTES + hashKeyBytes.readableBytes() + Integer.BYTES + rangeKeyBytes.readableBytes();
	}

	public static int combinedSize(final ByteBuf hashKeyBytes) {
		return Integer.BYTES + hashKeyBytes.readableBytes();
	}

	public static byte[] toByteArray(final ByteBuf buf) {
		final byte[] buffer = new byte[buf.readableBytes()];
		buf.readBytes(buffer);
		return buffer;
	}

	public static int compareKeys(final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator, final ByteBuffer k1, final ByteBuffer k2) {
		// First hash key
		int k1Index = 0;
		final int k1HashKeyLength = k1.getInt(k1Index);
		k1Index += Integer.BYTES;
		final ByteBuffer k1HashKeyBytes = slice(k1, k1Index, k1HashKeyLength);

		// Second hash key
		int k2Index = 0;
		final int k2HashKeyLength = k2.getInt(k2Index);
		k2Index += Integer.BYTES;
		final ByteBuffer k2HashKeyBytes = slice(k2, k2Index, k2HashKeyLength);

		final int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);

		if (rangeKeyComparator != null && hashComparison == 0) {
			// First range key
			k1Index += k1HashKeyLength;
			final int k1RangeKeyLength = k1.getInt(k1Index);
			k1Index += Integer.BYTES;
			final ByteBuffer k1RangeKeyBytes = slice(k1, k1Index, k1RangeKeyLength);

			// Second range key
			k2Index += k2HashKeyLength;
			final int k2RangeKeyLength = k2.getInt(k2Index);
			k2Index += Integer.BYTES;
			final ByteBuffer k2RangeKeyBytes = slice(k2, k2Index, k2RangeKeyLength);

			return rangeKeyComparator.compare(k1RangeKeyBytes, k2RangeKeyBytes);
		}

		return hashComparison;
	}

	public static int compareKeys(final Comparator<ByteBuffer> hashKeyComparator, final ByteBuffer k1,
			final ByteBuffer k2) {
		// First hash key
		final int k1Index = 0;
		final int k1HashKeyLength = k1.capacity();
		final ByteBuffer k1HashKeyBytes = slice(k1, k1Index, k1HashKeyLength);

		// Second hash key
		final int k2Index = 0;
		final int k2HashKeyLength = k2.capacity();
		final ByteBuffer k2HashKeyBytes = slice(k2, k2Index, k2HashKeyLength);

		final int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);

		return hashComparison;
	}

	public static <H, R> int compareKeys(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator,
			final ObjectRangeTableKey<H, R> k1, final ObjectRangeTableKey<H, R> k2) {
		return compareKeys(hashKeyComparator, rangeKeyComparator, k1.getHashKey(), k1.getRangeKey(), k2.getHashKey(),
				k2.getRangeKey());
	}

	public static <H, R> int compareKeys(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator,
			final H h1, final R r1, final H h2, final R r2) {
		// First hash key
		final int hashComparison = hashKeyComparator.compare(h1, h2);

		if (rangeKeyComparator != null && hashComparison == 0) {
			return rangeKeyComparator.compare(r1, r2);
		}

		return hashComparison;
	}

	public static <H, R> int compareKeys(final Comparator<H> hashKeyComparator, final H h1, final H h2) {
		final int hashComparison = hashKeyComparator.compare(h1, h2);
		return hashComparison;
	}

	public static <H, R> ObjectRangeTableKey<H, R> combine(final H hashKey, final R rangeKey,
			final Comparator<ObjectRangeTableKey<H, R>> comparator) {
		return new ObjectRangeTableKey<H, R>(hashKey, rangeKey, comparator);
	}

}
