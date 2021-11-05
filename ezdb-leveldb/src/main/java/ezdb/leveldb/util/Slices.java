package ezdb.leveldb.util;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;

import org.iq80.leveldb.util.Slice;

import ezdb.serde.Serde;
import ezdb.table.RawTableRow;
import ezdb.table.range.RawRangeTableRow;
import ezdb.util.LazyValueGetter;
import io.netty.buffer.ByteBuf;

public class Slices {

	public static ByteBuffer unwrap(final Slice slice) {
		return ByteBuffer.wrap(slice.getRawArray(), slice.getRawOffset(), slice.length()).slice();
	}

	public static ByteBuffer unwrapSlice(final Slice slice, final int index, final int length) {
		return ByteBuffer.wrap(slice.getRawArray(), slice.getRawOffset() + index, length).slice();
	}

	public static Slice wrap(final ByteBuf buffer) {
		return new Slice(buffer.array(), buffer.readerIndex() + buffer.arrayOffset(), buffer.readableBytes());
	}

	public static byte[] wrapCopy(final ByteBuf buffer) {
		final byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		return bytes;
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> newRawRangeTableRow(final Slice keyBuffer,
			final Slice valueBuffer, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {
		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyValueGetter<Entry<ByteBuffer, ByteBuffer>> hashKeyBytes_rangeKeyBytes = new LazyValueGetter<Entry<ByteBuffer, ByteBuffer>>() {
			@Override
			protected Entry<ByteBuffer, ByteBuffer> initialize() {
				final Slice compoundKeyBytes = keyBuffer;
				int index = 0;
				// leveldb stores data in little endian
				final int hashKeyBytesLength = Integer.reverseBytes(compoundKeyBytes.getInt(index));
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = unwrapSlice(compoundKeyBytes, index, hashKeyBytesLength);
				index += hashKeyBytesLength;
				final int rangeKeyBytesLength = Integer.reverseBytes(compoundKeyBytes.getInt(index));
				index += Integer.BYTES;
				final ByteBuffer rangeKeyBytes;

				if (rangeKeyBytesLength > 0) {
					rangeKeyBytes = unwrapSlice(compoundKeyBytes, index, rangeKeyBytesLength);
				} else {
					rangeKeyBytes = null;
				}
				return new Entry<ByteBuffer, ByteBuffer>() {
					@Override
					public ByteBuffer setValue(final ByteBuffer value) {
						throw new UnsupportedOperationException();
					}

					@Override
					public ByteBuffer getValue() {
						if (rangeKeyBytes != null) {
							rangeKeyBytes.clear();
						}
						return rangeKeyBytes;
					}

					@Override
					public ByteBuffer getKey() {
						hashKeyBytes.clear();
						return hashKeyBytes;
					}
				};
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get().getValue();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuffer valueBytes = unwrap(valueBuffer);
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, V> RawTableRow<H, V> newRawTableRow(final Slice keyBuffer, final Slice valueBuffer,
			final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = unwrap(keyBuffer);
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuffer valueBytes = unwrap(valueBuffer);
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static int compareKeys(final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator, final Slice k1, final Slice k2) {
		// First hash key
		int k1Index = 0;
		// leveldb stores data in little endian
		final int k1HashKeyLength = Integer.reverseBytes(k1.getInt(k1Index));
		k1Index += Integer.BYTES;
		final ByteBuffer k1HashKeyBytes = unwrapSlice(k1, k1Index, k1HashKeyLength);

		// Second hash key
		int k2Index = 0;
		final int k2HashKeyLength = Integer.reverseBytes(k2.getInt(k2Index));
		k2Index += Integer.BYTES;
		final ByteBuffer k2HashKeyBytes = unwrapSlice(k2, k2Index, k2HashKeyLength);

		final int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);

		if (rangeKeyComparator != null && hashComparison == 0) {
			// First range key
			k1Index += k1HashKeyLength;
			final int k1RangeKeyLength = Integer.reverseBytes(k1.getInt(k1Index));
			k1Index += Integer.BYTES;
			final ByteBuffer k1RangeKeyBytes = unwrapSlice(k1, k1Index, k1RangeKeyLength);

			// Second range key
			k2Index += k2HashKeyLength;
			final int k2RangeKeyLength = Integer.reverseBytes(k2.getInt(k2Index));
			k2Index += Integer.BYTES;
			final ByteBuffer k2RangeKeyBytes = unwrapSlice(k2, k2Index, k2RangeKeyLength);

			return rangeKeyComparator.compare(k1RangeKeyBytes, k2RangeKeyBytes);
		}

		return hashComparison;
	}

	public static int compareKeys(final Comparator<ByteBuffer> hashKeyComparator, final Slice k1, final Slice k2) {
		// First hash key
		final int k1Index = 0;
		// leveldb stores data in little endian
		final int k1HashKeyLength = k1.length();
		final ByteBuffer k1HashKeyBytes = unwrapSlice(k1, k1Index, k1HashKeyLength);

		// Second hash key
		final int k2Index = 0;
		final int k2HashKeyLength = k2.length();
		final ByteBuffer k2HashKeyBytes = unwrapSlice(k2, k2Index, k2HashKeyLength);

		final int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);
		return hashComparison;
	}

}
