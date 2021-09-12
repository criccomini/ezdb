package ezdb;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import ezdb.serde.Serde;
import ezdb.util.Util;

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
		final LazyGetter<Entry<ByteBuffer, ByteBuffer>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuffer, ByteBuffer>>() {
			@Override
			protected Entry<ByteBuffer, ByteBuffer> internalGet() {
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
				final ByteBuffer valueBytes = rawRow.getValue();
				return valueSerde.fromBuffer(valueBytes);
			}
		};
		return new RawTableRow<>(hashKey, rangeKey, value);
	}

	public static <H, R, V> RawTableRow<H, R, V> valueOfBytes(final Entry<byte[], byte[]> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {

		// extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<ByteBuffer, ByteBuffer>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<ByteBuffer, ByteBuffer>>() {
			@Override
			protected Entry<ByteBuffer, ByteBuffer> internalGet() {
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
				final byte[] valueBytes = rawRow.getValue();
				return valueSerde.fromBytes(valueBytes);
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
