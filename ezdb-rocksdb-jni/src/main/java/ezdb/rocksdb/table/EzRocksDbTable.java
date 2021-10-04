package ezdb.rocksdb.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import ezdb.DbException;
import ezdb.rocksdb.EzRocksDbFactory;
import ezdb.rocksdb.util.EzDBIterator;
import ezdb.rocksdb.util.RocksDBJniDBIterator;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

public class EzRocksDbTable<H, V> implements Table<H, V> {
	private final RocksDB db;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Options options;

	public EzRocksDbTable(final File path, final EzRocksDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;

		this.options = new Options();

		options.setCreateIfMissing(true);
		options.setComparator(new EzRocksDbComparator(hashKeyComparator));

		try {
			this.db = factory.open(path, options);
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		try {
			db.put(hashKeySerde.toBytes(hashKey), valueSerde.toBytes(value));
		} catch (final RocksDBException e) {
			throw new DbException(e);
		}
	}

	@Override
	public V get(final H hashKey) {
		byte[] valueBytes;
		try {
			valueBytes = db.get(hashKeySerde.toBytes(hashKey));
		} catch (final RocksDBException e) {
			throw new DbException(e);
		}

		if (valueBytes == null) {
			return null;
		}

		return valueSerde.fromBytes(valueBytes);
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final EzDBIterator<H, V> iterator = new RocksDBJniDBIterator<H, V>(db.newIterator(), hashKeySerde, valueSerde);
		iterator.seekToFirst();
		return new AutoClosingTableIterator<H, V>(new TableIterator<TableRow<H, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public TableRow<H, V> next() {
				if (hasNext()) {
					return iterator.next();
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public void close() {
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public void delete(final H hashKey) {
		try {
			this.db.delete(hashKeySerde.toBytes(hashKey));
		} catch (final RocksDBException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void close() {
		try {
			this.db.close();
			this.options.close();
		} catch (final Exception e) {
			throw new DbException(e);
		}
	}

	private static class AutoClosingTableIterator<_H, _V> implements TableIterator<TableRow<_H, _V>> {

		private final TableIterator<TableRow<_H, _V>> delegate;
		private boolean closed;

		public AutoClosingTableIterator(final TableIterator<TableRow<_H, _V>> delegate) {
			this.delegate = delegate;
		}

		@Override
		public boolean hasNext() {
			if (closed) {
				return false;
			}
			final boolean hasNext = delegate.hasNext();
			if (!hasNext) {
				close();
			}
			return hasNext;
		}

		@Override
		public TableRow<_H, _V> next() {
			if (closed) {
				throw new NoSuchElementException();
			}
			return delegate.next();
		}

		@Override
		public void remove() {
			delegate.remove();
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void finalize() throws Throwable {
			super.finalize();
			close();
		}

		@Override
		public void close() {
			if (!closed) {
				closed = true;
				delegate.close();
			}
		}

	}

	@Override
	public Batch<H, V> newBatch() {
		return new EzRocksDbBatch<H, V>(db, hashKeySerde, valueSerde);
	}

}
