package ezdb;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import ezdb.serde.Serde;

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
		;
	}

	public RawTableRow(final Entry<byte[], byte[]> rawRow,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {

		//extract hashKeyBytes/rangeKeyBytes only if needed
		final LazyGetter<Entry<byte[], byte[]>> hashKeyBytes_rangeKeyBytes = new LazyGetter<Entry<byte[], byte[]>>() {
			@Override
			protected Entry<byte[], byte[]> internalGet() {
				final byte[] compoundKeyBytes = rawRow.getKey();
				final ByteBuffer keyBuffer = ByteBuffer.wrap(compoundKeyBytes);
				final int hashKeyBytesLength = keyBuffer.getInt();
				final byte[] hashKeyBytes = new byte[hashKeyBytesLength];
				keyBuffer.get(hashKeyBytes);
				final int rangeKeyBytesLength = keyBuffer.getInt();

				final byte[] rangeKeyBytes;
				if (rangeKeyBytesLength > 0) {
					rangeKeyBytes = new byte[rangeKeyBytesLength];
					keyBuffer.get(rangeKeyBytes);
				} else {
					rangeKeyBytes = null;
				}
				return new Entry<byte[], byte[]>() {
					@Override
					public byte[] setValue(byte[] value) {
						throw new UnsupportedOperationException();
					}

					@Override
					public byte[] getValue() {
						return rangeKeyBytes;
					}

					@Override
					public byte[] getKey() {
						return hashKeyBytes;
					}
				};
			}
		};

		rangeKey = new LazyGetter<R>() {
			@Override
			protected R internalGet() {
				byte[] rangeKeyBytes = hashKeyBytes_rangeKeyBytes.get()
						.getValue();
				if (rangeKeyBytes == null) {
					return null;
				} else {
					return rangeKeySerde.fromBytes(rangeKeyBytes);
				}
			}
		};

		hashKey = new LazyGetter<H>() {
			@Override
			protected H internalGet() {
				byte[] hashKeyBytes = hashKeyBytes_rangeKeyBytes.get().getKey();
				return hashKeySerde.fromBytes(hashKeyBytes);
			}
		};
		value = new LazyGetter<V>() {
			@Override
			protected V internalGet() {
				return valueSerde.fromBytes(rawRow.getValue());
			}
		};
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
		result = prime * result
				+ ((getHashKey() == null) ? 0 : getHashKey().hashCode());
		result = prime * result
				+ ((getRangeKey() == null) ? 0 : getRangeKey().hashCode());
		result = prime * result
				+ ((getValue() == null) ? 0 : getValue().hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RawTableRow other = (RawTableRow) obj;
		if (getHashKey() == null) {
			if (other.getHashKey() != null)
				return false;
		} else if (!getHashKey().equals(other.getHashKey()))
			return false;
		if (getRangeKey() == null) {
			if (other.getRangeKey() != null)
				return false;
		} else if (!getRangeKey().equals(other.getRangeKey()))
			return false;
		if (getValue() == null) {
			if (other.getValue() != null)
				return false;
		} else if (!getValue().equals(other.getValue()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [hashKey=" + getHashKey()
				+ ", rangeKey=" + getRangeKey() + ", value=" + getValue() + "]";
	}
}
