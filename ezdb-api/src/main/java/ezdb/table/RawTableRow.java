package ezdb.table;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Supplier;

import ezdb.serde.Serde;
import ezdb.util.LazyValueGetter;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

public class RawTableRow<H, V> implements TableRow<H, V> {
	private final Supplier<H> hashKey;
	private final Supplier<V> value;

	public RawTableRow(final H hashKey, final V value) {
		this.hashKey = () -> hashKey;
		this.value = () -> value;
	}

	public RawTableRow(final Supplier<H> hashKey, final Supplier<V> value) {
		this.hashKey = hashKey;
		this.value = value;
	}

	public static <H, V> RawTableRow<H, V> valueOfBuffer(final ByteBuffer keyBuffer, final ByteBuffer valueBuffer,
			final Serde<H> hashKeySerde, final Serde<V> valueSerde) {

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer compoundKeyBytes = keyBuffer;
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = Util.slice(compoundKeyBytes, index, hashKeyBytesLength);
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuffer valueBytes = valueBuffer;
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static <H, V> RawTableRow<H, V> valueOfBuffer(final Entry<ByteBuffer, ByteBuffer> rawRow,
			final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer compoundKeyBytes = rawRow.getKey();
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = Util.slice(compoundKeyBytes, index, hashKeyBytesLength);
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuffer valueBytes = rawRow.getValue();
				valueBytes.clear();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static <H, V> RawTableRow<H, V> valueOfBuf(final ByteBuf keyBuffer, final ByteBuf valueBuffer,
			final Serde<H> hashKeySerde, final Serde<V> valueSerde) {

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuf compoundKeyBytes = keyBuffer;
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf hashKeyBytes = compoundKeyBytes.slice(index, hashKeyBytesLength);
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuf valueBytes = valueBuffer;
				valueBytes.resetReaderIndex();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static <H, V> RawTableRow<H, V> valueOfBuf(final Entry<ByteBuf, ByteBuf> rawRow, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde) {
		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuf compoundKeyBytes = rawRow.getKey();
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf hashKeyBytes = compoundKeyBytes.slice(index, hashKeyBytesLength);
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuf valueBytes = rawRow.getValue();
				valueBytes.resetReaderIndex();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static <H, V> RawTableRow<H, V> valueOfBytes(final byte[] keyBuffer, final byte[] valueBuffer,
			final Serde<H> hashKeySerde, final Serde<V> valueSerde) {

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer compoundKeyBytes = ByteBuffer.wrap(keyBuffer);
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = Util.slice(compoundKeyBytes, index, hashKeyBytesLength);
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final byte[] valueBytes = valueBuffer;
				return valueSerde.fromBytes(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	public static <H, V> RawTableRow<H, V> valueOfBytes(final Entry<byte[], byte[]> rawRow, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde) {

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer compoundKeyBytes = ByteBuffer.wrap(rawRow.getKey());
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuffer hashKeyBytes = Util.slice(compoundKeyBytes, index, hashKeyBytesLength);
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final byte[] valueBytes = rawRow.getValue();
				return valueSerde.fromBytes(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, value);
	}

	@Override
	public H getHashKey() {
		return hashKey.get();
	}

	@Override
	public V getValue() {
		return value.get();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getHashKey() == null) ? 0 : getHashKey().hashCode());
		result = prime * result + ((getValue() == null) ? 0 : getValue().hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final RawTableRow other = (RawTableRow) obj;
		if (getHashKey() == null) {
			if (other.getHashKey() != null) {
				return false;
			}
		} else if (!getHashKey().equals(other.getHashKey())) {
			return false;
		}
		if (getValue() == null) {
			if (other.getValue() != null) {
				return false;
			}
		} else if (!getValue().equals(other.getValue())) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [hashKey=" + getHashKey() + ", value=" + getValue() + "]";
	}
}
