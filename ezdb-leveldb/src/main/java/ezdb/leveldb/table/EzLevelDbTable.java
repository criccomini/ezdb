package ezdb.leveldb.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.ExtendedDbImpl;
import org.iq80.leveldb.util.Slice;

import ezdb.DbException;
import ezdb.leveldb.EzLevelDbJavaFactory;
import ezdb.leveldb.util.EzLevelDBIterator;
import ezdb.leveldb.util.Slices;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLevelDbTable<H, V> implements Table<H, V> {

	private static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

	private final ReadOptions defaultReadOptions;

	private final ExtendedDbImpl db;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;

	public EzLevelDbTable(final File path, final EzLevelDbJavaFactory factory, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;

		final Options options = new Options();
		options.createIfMissing(true);
		this.defaultReadOptions = new ReadOptions().verifyChecksums(factory.isVerifyChecksums());
		options.comparator(new EzLevelDbJavaComparator(hashKeyComparator));

		try {
			this.db = factory.open(path, options);
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final EzLevelDBIterator<H, V> iterator = new EzLevelDBIterator<H, V>(db.extendedIterator(defaultReadOptions),
				hashKeySerde, valueSerde);
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
	public void put(final H hashKey, final V value) {
		db.put(hashKeySerde.toBytes(hashKey), valueSerde.toBytes(value), DEFAULT_WRITE_OPTIONS);
	}

	@Override
	public V get(final H hashKey) {
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
		try {
			hashKeySerde.toBuffer(keyBuffer, hashKey);
			final Slice valueBytes = db.get(Slices.wrap(keyBuffer), defaultReadOptions);

			if (valueBytes == null) {
				return null;
			}

			return valueSerde.fromBuffer(Slices.unwrap(valueBytes));
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
		}
	}

	@Override
	public void delete(final H hashKey) {
		this.db.delete(hashKeySerde.toBytes(hashKey), DEFAULT_WRITE_OPTIONS);
	}

	@Override
	public void close() {
		try {
			this.db.close();
		} catch (final Exception e) {
			throw new DbException(e);
		}
	}

	@Override
	public Batch<H, V> newBatch() {
		return new EzLevelDbJavaBatch<H, V>(db, hashKeySerde, valueSerde);
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

}
