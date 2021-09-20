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

		final ByteBuf hashKeyBytes = buffer.alloc().buffer();
		hashKeySerde.toBuffer(hashKeyBytes, hashKey);
		final ByteBuf rangeKeyBytes;
		if (rangeKey != null) {
			rangeKeyBytes = buffer.alloc().buffer();
			rangeKeySerde.toBuffer(rangeKeyBytes, rangeKey);
		} else {
			rangeKeyBytes = buffer.alloc().buffer(0);
		}

		combineBuf(buffer, hashKeyBytes, rangeKeyBytes);

		hashKeyBytes.release(hashKeyBytes.refCnt());
		rangeKeyBytes.release(rangeKeyBytes.refCnt());
	}

	public static <H, R> ByteBuffer combineBuffer(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final H hashKey, final R rangeKey) {

		final ByteBuf hashKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer();
		hashKeySerde.toBuffer(hashKeyBytes, hashKey);
		final ByteBuf rangeKeyBytes;
		if (rangeKey != null) {
			rangeKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer();
			rangeKeySerde.toBuffer(rangeKeyBytes, rangeKey);
		} else {
			rangeKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer(0);
		}

		final ByteBuffer buffer = ByteBuffer.allocate(combinedSize(hashKeyBytes, rangeKeyBytes));
		combineBuffer(buffer, hashKeyBytes, rangeKeyBytes);

		hashKeyBytes.release(hashKeyBytes.refCnt());
		rangeKeyBytes.release(rangeKeyBytes.refCnt());

		buffer.clear();
		return buffer;
	}

	public static <H, R> byte[] combineBytes(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final H hashKey,
			final R rangeKey) {

		final ByteBuf hashKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer();
		hashKeySerde.toBuffer(hashKeyBytes, hashKey);
		final ByteBuf rangeKeyBytes;
		if (rangeKey != null) {
			rangeKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer();
			rangeKeySerde.toBuffer(rangeKeyBytes, rangeKey);
		} else {
			rangeKeyBytes = ByteBufAllocator.DEFAULT.heapBuffer(0);
		}

		final byte[] bytes = combineBytes(hashKeyBytes, rangeKeyBytes);

		hashKeyBytes.release(hashKeyBytes.refCnt());
		rangeKeyBytes.release(rangeKeyBytes.refCnt());

		return bytes;
	}

	public static int combinedSize(final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		return Integer.BYTES + hashKeyBytes.readableBytes() + Integer.BYTES + rangeKeyBytes.readableBytes();
	}

	public static void combineBuf(final ByteBuf buffer, final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		final int requiredCapacity = combinedSize(hashKeyBytes, rangeKeyBytes);
		if (buffer.capacity() < requiredCapacity) {
			buffer.capacity(requiredCapacity);
		}
		buffer.writeInt(hashKeyBytes.readableBytes());
		buffer.writeBytes(hashKeyBytes);
		buffer.writeInt(rangeKeyBytes.readableBytes());
		buffer.writeBytes(rangeKeyBytes);
	}

	public static ByteBuffer combineBuffer(final ByteBuffer buffer, final ByteBuf hashKeyBytes,
			final ByteBuf rangeKeyBytes) {
		buffer.putInt(hashKeyBytes.readableBytes());
		hashKeyBytes.readBytes(buffer.limit(buffer.position() + hashKeyBytes.readableBytes()));
		buffer.limit(buffer.capacity());
		buffer.putInt(rangeKeyBytes.readableBytes());
		rangeKeyBytes.readBytes(buffer);
		return buffer;
	}

	/**
	 * Utility function to combine a hash key and range key. Hash/range key pairs
	 * are expected to be persisted in the following byte format:
	 * 
	 * <pre>
	 * [4 byte hash key length]
	 * [arbitrary hash key bytes]
	 * [4 byte range key length]
	 * [arbitrary range key bytes]
	 * </pre>
	 * 
	 * @param hashKeyBytes  Are the hash key's bytes.
	 * @param rangeKeyBytes Are the range key's bytes.
	 * @return Returns a byte array defined by the format above.
	 */
	public static byte[] combineBytes(final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		final ByteBuffer buffer = ByteBuffer.allocate(combinedSize(hashKeyBytes, rangeKeyBytes));
		combineBuffer(buffer, hashKeyBytes, rangeKeyBytes);
		return buffer.array();
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

	public static <H, R> int compareKeys(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator,
			final ObjectTableKey<H, R> k1, final ObjectTableKey<H, R> k2) {
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

	public static <H, R> ObjectTableKey<H, R> combine(final H hashKey, final R rangeKey,
			final Comparator<ObjectTableKey<H, R>> comparator) {
		return new ObjectTableKey<H, R>(hashKey, rangeKey, comparator);
	}

}
