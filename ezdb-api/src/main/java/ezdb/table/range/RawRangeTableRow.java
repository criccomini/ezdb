package ezdb.table.range;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.function.Supplier;

import ezdb.serde.Serde;
import ezdb.table.RangeTableRow;
import ezdb.util.LazyRangeKeysGetter;
import ezdb.util.LazyValueGetter;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

public class RawRangeTableRow<H, R, V> implements RangeTableRow<H, R, V> {
	private final Supplier<H> hashKey;
	private final Supplier<R> rangeKey;
	private final Supplier<V> value;

	public RawRangeTableRow(final H hashKey, final R rangeKey, final V value) {
		this.hashKey = () -> hashKey;
		this.rangeKey = () -> rangeKey;
		this.value = () -> value;
	}

	public RawRangeTableRow(final Supplier<H> hashKey, final Supplier<R> rangeKey, final Supplier<V> value) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		this.value = value;
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBuffer(final ByteBuffer keyBuffer, final ByteBuffer valueBuffer,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuffer, ByteBuffer> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuffer, ByteBuffer>() {
			@Override
			protected void initialize() {
				final ByteBuffer compoundKeyBytes = keyBuffer;
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

				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.clear();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final ByteBuffer valueBytes = valueBuffer;
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBuffer(final Entry<ByteBuffer, ByteBuffer> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuffer, ByteBuffer> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuffer, ByteBuffer>() {
			@Override
			protected void initialize() {
				final ByteBuffer compoundKeyBytes = rawRow.getKey();
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

				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.clear();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
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
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBuf(final ByteBuf keyBuffer, final ByteBuf valueBuffer,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuf, ByteBuf> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuf, ByteBuf>() {
			@Override
			protected void initialize() {
				final ByteBuf compoundKeyBytes = keyBuffer;
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf hashKeyBytes = compoundKeyBytes.slice(index, hashKeyBytesLength);
				index += hashKeyBytesLength;
				final int rangeKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf rangeKeyBytes;

				if (rangeKeyBytesLength > 0) {
					rangeKeyBytes = compoundKeyBytes.slice(index, rangeKeyBytesLength);
				} else {
					rangeKeyBytes = null;
				}

				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuf hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuf rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.resetReaderIndex();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
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
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBuf(final Entry<ByteBuf, ByteBuf> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuf, ByteBuf> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuf, ByteBuf>() {
			@Override
			protected void initialize() {
				final ByteBuf compoundKeyBytes = rawRow.getKey();
				int index = 0;
				final int hashKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf hashKeyBytes = compoundKeyBytes.slice(index, hashKeyBytesLength);
				index += hashKeyBytesLength;
				final int rangeKeyBytesLength = compoundKeyBytes.getInt(index);
				index += Integer.BYTES;
				final ByteBuf rangeKeyBytes;

				if (rangeKeyBytesLength > 0) {
					rangeKeyBytes = compoundKeyBytes.slice(index, rangeKeyBytesLength);
				} else {
					rangeKeyBytes = null;
				}

				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuf hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuf rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.resetReaderIndex();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
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
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBytes(final byte[] keyBuffer, final byte[] valueBuffer,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuffer, ByteBuffer> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuffer, ByteBuffer>() {
			@Override
			protected void initialize() {
				final ByteBuffer compoundKeyBytes = ByteBuffer.wrap(keyBuffer);
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
				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.clear();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final byte[] valueBytes = valueBuffer;
				return valueSerde.fromBytes(valueBytes);
			}
		};
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawRangeTableRow<H, R, V> valueOfBytes(final Entry<byte[], byte[]> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyRangeKeysGetter<ByteBuffer, ByteBuffer> hashKeyBytes_rangeKeyBytes = new LazyRangeKeysGetter<ByteBuffer, ByteBuffer>() {
			@Override
			protected void initialize() {
				final ByteBuffer compoundKeyBytes = ByteBuffer.wrap(rawRow.getKey());
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
				this.hashKey = hashKeyBytes;
				this.rangeKey = rangeKeyBytes;
			}
		};

		final LazyValueGetter<H> hashKey = new LazyValueGetter<H>() {
			@Override
			protected H initialize() {
				final ByteBuffer hashKeyBytes = hashKeyBytes_rangeKeyBytes.getHashKey();
				hashKeyBytes.clear();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyValueGetter<R> rangeKey = new LazyValueGetter<R>() {
			@Override
			protected R initialize() {
				final ByteBuffer rangeKeyBytes = hashKeyBytes_rangeKeyBytes.getRangeKey();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.clear();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyValueGetter<V> value = new LazyValueGetter<V>() {
			@Override
			protected V initialize() {
				final byte[] valueBytes = rawRow.getValue();
				return valueSerde.fromBytes(valueBytes);
			}
		};
		return new RawRangeTableRow<>(hashKey, rangeKey, value);
	}

	@Override
	public H getHashKey() {
		return hashKey.get();
	}

	@Override
	public R getRangeKey() {
		return rangeKey.get();
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
		result = prime * result + ((getRangeKey() == null) ? 0 : getRangeKey().hashCode());
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
		final RawRangeTableRow other = (RawRangeTableRow) obj;
		if (getHashKey() == null) {
			if (other.getHashKey() != null) {
				return false;
			}
		} else if (!getHashKey().equals(other.getHashKey())) {
			return false;
		}
		if (getRangeKey() == null) {
			if (other.getRangeKey() != null) {
				return false;
			}
		} else if (!getRangeKey().equals(other.getRangeKey())) {
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
		return getClass().getSimpleName() + " [hashKey=" + getHashKey() + ", rangeKey=" + getRangeKey() + ", value="
				+ getValue() + "]";
	}
}
