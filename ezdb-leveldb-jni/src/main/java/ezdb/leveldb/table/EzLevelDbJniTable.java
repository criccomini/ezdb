package ezdb.leveldb.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import ezdb.DbException;
import ezdb.leveldb.EzLevelDbJniFactory;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.RawTableRow;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;

public class EzLevelDbJniTable<H, V> implements Table<H, V> {
	private final DB db;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;

	public EzLevelDbJniTable(final File path, final EzLevelDbJniFactory factory, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;

		final Options options = new Options();
		options.createIfMissing(true);
		options.comparator(new EzLevelDbJniComparator(hashKeyComparator));

		try {
			this.db = factory.open(path, options);
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		db.put(hashKeySerde.toBytes(hashKey), valueSerde.toBytes(value));
	}

	@Override
	public V get(final H hashKey) {
		final byte[] valueBytes = db.get(hashKeySerde.toBytes(hashKey));

		if (valueBytes == null) {
			return null;
		}

		return valueSerde.fromBytes(valueBytes);
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final DBIterator iterator = db.iterator();
		iterator.seekToFirst();
		return new AutoClosingTableIterator<H, V>(new TableIterator<TableRow<H, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public TableRow<H, V> next() {
				if (hasNext()) {
					return RawTableRow.valueOfBytes(iterator.next(), hashKeySerde, valueSerde);
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
		this.db.delete(hashKeySerde.toBytes(hashKey));
	}

	@Override
	public void close() {
		try {
			this.db.close();
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
		return new EzLevelDbJniBatch<H, V>(db, hashKeySerde, valueSerde);
	}

}
