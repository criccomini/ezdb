package ezdb.lmdb.table;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import ezdb.DbException;
import ezdb.lmdb.EzLmDbFactory;
import ezdb.lmdb.util.EzDBIterator;
import ezdb.lmdb.util.LmDBJnrDBIterator;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

public class EzLmDbTable<H, V> implements Table<H, V> {
	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;

	public EzLmDbTable(final File path, final EzLmDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;

		try {
			this.env = factory.create(path.getParentFile(), EnvFlags.MDB_NOTLS, EnvFlags.MDB_WRITEMAP,
					EnvFlags.MDB_NOMEMINIT, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC);
		} catch (final IOException e) {
			throw new DbException(e);
		}
		final EzLmDbComparator comparator = new EzLmDbComparator(hashKeyComparator);
		try {
			this.db = factory.open(path.getName(), env, comparator, false, DbiFlags.MDB_CREATE);
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		hashKeySerde.toBuffer(keyBuffer, hashKey);
		final ByteBuf valueBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		valueSerde.toBuffer(valueBuffer, value);
		try {
			db.put(keyBuffer.nioBuffer(), valueBuffer.nioBuffer());
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
			valueBuffer.release(valueBuffer.refCnt());
		}
	}

	@Override
	public V get(final H hashKey) {
		final Txn<ByteBuffer> txn = env.txnRead();
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		try {
			hashKeySerde.toBuffer(keyBuffer, hashKey);
			final ByteBuffer valueBytes = db.get(txn, keyBuffer.nioBuffer());

			if (valueBytes == null) {
				return null;
			}

			return valueSerde.fromBuffer(Unpooled.wrappedBuffer(valueBytes));
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
			txn.close();
		}
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final EzDBIterator<H, V> iterator = new LmDBJnrDBIterator<H, V>(env, db, hashKeySerde, valueSerde);
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
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
		hashKeySerde.toBuffer(buffer, hashKey);
		try {
			this.db.delete(buffer.nioBuffer());
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public void close() {
		try {
			this.db.close();
			this.env.close();
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
		return new EzLmDbBatch<H, V>(env, db, hashKeySerde, valueSerde);
	}

}
