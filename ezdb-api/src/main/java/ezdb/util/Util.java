package ezdb.util;

import java.util.Comparator;

import ezdb.serde.Serde;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

public class Util {

	public static <H, R> void combine(final ByteBuf buffer, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final H hashKey, final R rangeKey) {

		final ByteBuf hashKeyBytes = buffer.alloc().buffer();
		hashKeySerde.toBuffer(hashKeyBytes, hashKey);
		final ByteBuf rangeKeyBytes;
		if (rangeKey != null) {
			rangeKeyBytes = buffer.alloc().buffer();
			rangeKeySerde.toBuffer(rangeKeyBytes, rangeKey);
		} else {
			rangeKeyBytes = buffer.alloc().buffer(0);
		}

		combine(buffer, hashKeyBytes, rangeKeyBytes);

		hashKeyBytes.release(hashKeyBytes.refCnt());
		if (rangeKeyBytes != null) {
			rangeKeyBytes.release(rangeKeyBytes.refCnt());
		}
	}

	public static <H, R> byte[] combine(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final H hashKey,
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

		final byte[] bytes = combine(hashKeyBytes, rangeKeyBytes);

		hashKeyBytes.release(hashKeyBytes.refCnt());
		rangeKeyBytes.release(rangeKeyBytes.refCnt());

		return bytes;
	}

	public static void combine(final ByteBuf buffer, final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		final int requiredCapacity = Integer.BYTES + hashKeyBytes.readableBytes() + Integer.BYTES
				+ rangeKeyBytes.readableBytes();
		if (buffer.capacity() < requiredCapacity) {
			buffer.capacity(requiredCapacity);
		}
		buffer.writeInt(hashKeyBytes.readableBytes());
		buffer.writeBytes(hashKeyBytes);
		buffer.writeInt(rangeKeyBytes.readableBytes());
		buffer.writeBytes(rangeKeyBytes);
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
	public static byte[] combine(final ByteBuf hashKeyBytes, final ByteBuf rangeKeyBytes) {
		final byte[] bytes = new byte[Integer.BYTES + hashKeyBytes.readableBytes() + Integer.BYTES
				+ rangeKeyBytes.readableBytes()];
		final ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		buf.clear();
		combine(buf, hashKeyBytes, rangeKeyBytes);
		return bytes;
	}

	public static int compareKeys(final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator, final byte[] k1, final byte[] k2) {
		return compareKeys(hashKeyComparator, rangeKeyComparator, Unpooled.wrappedBuffer(k1),
				Unpooled.wrappedBuffer(k2));
	}

	public static int compareKeys(final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator, final ByteBuf k1, final ByteBuf k2) {
		// First hash key
		int k1Index = 0;
		final int k1HashKeyLength = k1.getInt(k1Index);
		k1Index += Integer.BYTES;
		final ByteBuf k1HashKeyBytes = k1.slice(k1Index, k1HashKeyLength);

		// Second hash key
		int k2Index = 0;
		final int k2HashKeyLength = k2.getInt(k2Index);
		k2Index += Integer.BYTES;
		final ByteBuf k2HashKeyBytes = k2.slice(k2Index, k2HashKeyLength);

		final int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);

		if (rangeKeyComparator != null && hashComparison == 0) {
			// First range key
			k1Index += k1HashKeyLength;
			final int k1RangeKeyLength = k1.getInt(k1Index);
			k1Index += Integer.BYTES;
			final ByteBuf k1RangeKeyBytes = k1.slice(k1Index, k1RangeKeyLength);

			// Second range key
			k2Index += k2HashKeyLength;
			final int k2RangeKeyLength = k2.getInt(k2Index);
			k2Index += Integer.BYTES;
			final ByteBuf k2RangeKeyBytes = k2.slice(k2Index, k2RangeKeyLength);

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

	public static <H, R> ObjectTableKey<H, R> combine(final H hashKey, final R rangeKey) {
		return new ObjectTableKey<H, R>(hashKey, rangeKey);
	}
}
