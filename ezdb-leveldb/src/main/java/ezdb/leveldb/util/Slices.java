package ezdb.leveldb.util;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;

import org.iq80.leveldb.util.Slice;

import ezdb.LazyGetter;
import ezdb.RawTableRow;
import ezdb.TableRow;
import ezdb.serde.Serde;

public class Slices {

	public static ByteBuffer unwrap(final Slice slice) {
		return ByteBuffer.wrap(slice.getRawArray(), slice.getRawOffset(), slice.length());
	}

	public static ByteBuffer unwrapSlice(final Slice slice, final int index, final int length) {
		return ByteBuffer.wrap(slice.getRawArray(), slice.getRawOffset() + index, length).slice();
	}

	public static Slice wrap(final ByteBuffer buffer) {
		return new Slice(buffer.array(), buffer.position(), buffer.remaining());
	}

	public static <H, R, V> TableRow<H, R, V> newRawTableRow(final Entry<Slice, Slice> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<ByteBuffer, ByteBuffer>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuffer, ByteBuffer>>() {
			@Override
			protected Entry<ByteBuffer, ByteBuffer> internalGet() {
				final Slice compoundKeyBytes = rawRow.getKey();
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

		final LazyGetter<H> hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyGetter<R> rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get().getValue();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyGetter<V> value = new LazyGetter<V>() {
			@Override
			protected V internalGet() {
				final ByteBuffer valueBytes = unwrap(rawRow.getValue());
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, rangeKey, value);
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

}
