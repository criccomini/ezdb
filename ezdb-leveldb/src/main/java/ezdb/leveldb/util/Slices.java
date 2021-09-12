package ezdb.leveldb.util;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.iq80.leveldb.util.Slice;

import ezdb.LazyGetter;
import ezdb.RawTableRow;
import ezdb.TableRow;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class Slices {

	public static ByteBuffer unwrap(final Slice slice) {
		return ByteBuffer.wrap(slice.getRawArray(), slice.getRawOffset(), slice.length());
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
				final ByteBuffer compoundKeyBytes = unwrap(rawRow.getKey());
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = Util.slice(compoundKeyBytes, index, hashKeyBytesLength);
				index += hashKeyBytesLength;
				final int rangeKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer rangeKeyBytes;

				if (rangeKeyBytesLength > 0) {
					rangeKeyBytes = Util.slice(compoundKeyBytes, index, rangeKeyBytesLength);
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

}
