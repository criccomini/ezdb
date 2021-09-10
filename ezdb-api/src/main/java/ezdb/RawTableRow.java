package ezdb;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import ezdb.serde.Serde;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class RawTableRow<H, R, V> implements TableRow<H, R, V> {
	private final LazyGetter<H> hashKey;
	private final LazyGetter<R> rangeKey;
	private final LazyGetter<V> value;

	public RawTableRow(final H hashKey, final R rangeKey, final V value) {
		this.hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				return hashKey;
			}
		};
		this.rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				return rangeKey;
			}
		};
		;
		this.value = new LazyGetter<V>() {
			@Override
			protected V internalGet() {
				return value;
			}
		};
	}

	public RawTableRow(final LazyGetter<H> hashKey, final LazyGetter<R> rangeKey, final LazyGetter<V> value) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		this.value = value;
	}

	public static <H, R, V> RawTableRow<H, R, V> valueOfBuffer(final Entry<ByteBuffer, ByteBuffer> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<ByteBuf, ByteBuf>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuf, ByteBuf>>() {
			@Override
			protected Entry<ByteBuf, ByteBuf> internalGet() {
				final ByteBuf compoundKeyBytes = Unpooled.wrappedBuffer(rawRow.getKey());
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
				return new Entry<ByteBuf, ByteBuf>() {
					@Override
					public ByteBuf setValue(final ByteBuf value) {
						throw new UnsupportedOperationException();
					}

					@Override
					public ByteBuf getValue() {
						return rangeKeyBytes;
					}

					@Override
					public ByteBuf getKey() {
						return hashKeyBytes;
					}
				};
			}
		};

		final LazyGetter<H> hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				final ByteBuf hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyGetter<R> rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				final ByteBuf rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get().getValue();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.resetReaderIndex();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyGetter<V> value = new LazyGetter<V>() {
			@Override
			protected V internalGet() {
				final ByteBuf valueBytes = Unpooled.wrappedBuffer(rawRow.getValue());
				valueBytes.resetReaderIndex();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawTableRow<H, R, V> valueOfBuf(final Entry<ByteBuf, ByteBuf> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<ByteBuf, ByteBuf>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuf, ByteBuf>>() {
			@Override
			protected Entry<ByteBuf, ByteBuf> internalGet() {
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
				return new Entry<ByteBuf, ByteBuf>() {
					@Override
					public ByteBuf setValue(final ByteBuf value) {
						throw new UnsupportedOperationException();
					}

					@Override
					public ByteBuf getValue() {
						return rangeKeyBytes;
					}

					@Override
					public ByteBuf getKey() {
						return hashKeyBytes;
					}
				};
			}
		};

		final LazyGetter<H> hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				final ByteBuf hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				hashKeyBytes.resetReaderIndex();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyGetter<R> rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				final ByteBuf rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get().getValue();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					rangeKeyBytes.resetReaderIndex();
					return rangeKeySerde.fromBuffer(rangeKeyBytes);
				}
			}
		};
		final LazyGetter<V> value = new LazyGetter<V>() {
			@Override
			protected V internalGet() {
				final ByteBuf valueBytes = rawRow.getValue();
				valueBytes.resetReaderIndex();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawTableRow<H, R, V> valueOfBytes(final Entry<byte[], byte[]> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<ByteBuf, ByteBuf>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuf, ByteBuf>>() {
			@Override
			protected Entry<ByteBuf, ByteBuf> internalGet() {
				final ByteBuf compoundKeyBytes = Unpooled.wrappedBuffer(rawRow.getKey());
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
				return new Entry<ByteBuf, ByteBuf>() {
					@Override
					public ByteBuf setValue(final ByteBuf value) {
						throw new UnsupportedOperationException();
					}

					@Override
					public ByteBuf getValue() {
						return rangeKeyBytes;
					}

					@Override
					public ByteBuf getKey() {
						return hashKeyBytes;
					}
				};
			}
		};

		final LazyGetter<H> hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				final ByteBuf hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				return hashKeySerde.fromBuffer(hashKeyBytes);
			}
		};
		final LazyGetter<R> rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				final ByteBuf rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get().getValue();
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
				return valueSerde.fromBytes(rawRow.getValue());
			}
		};
		return new RawTableRow<>(hashKey, rangeKey, value);
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
		final RawTableRow other = (RawTableRow) obj;
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
