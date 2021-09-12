package ezdb.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.lmdb.util.DBIterator;
import ezdb.lmdb.util.LmDBJnrDBIterator;
import ezdb.serde.Serde;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLmDbTable<H, R, V> implements RangeTable<H, R, V> {
	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzLmDbTable(final File path, final EzLmDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;

		try {
			this.env = factory.create(path.getParentFile(), EnvFlags.MDB_NOTLS, EnvFlags.MDB_WRITEMAP,
					EnvFlags.MDB_NOMEMINIT, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC);
		} catch (final IOException e) {
			throw new DbException(e);
		}
		final EzLmDbComparator comparator = new EzLmDbComparator(hashKeyComparator, rangeKeyComparator);
		try {
			this.db = factory.open(path.getName(), env, comparator, DbiFlags.MDB_CREATE);
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		put(hashKey, null, value);
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
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
		return get(hashKey, null);
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final Txn<ByteBuffer> txn = env.txnRead();
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		try {
			Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			final ByteBuffer valueBytes = db.get(txn, keyBuffer.nioBuffer());

			if (valueBytes == null) {
				return null;
			}

			return valueSerde.fromBuffer(valueBytes);
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
			txn.close();
		}
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		iterator.seek(keyBytesFromBuffer);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer,
						iterator.peekNext().getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		iterator.seek(keyBytesFromBuffer);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer,
						iterator.peekNext().getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final ByteBuf keyBytesTo = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesToBuffer = keyBytesTo.nioBuffer();
		iterator.seek(keyBytesFromBuffer);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesToBuffer,
						iterator.peekNext().getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				peek) -> Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peek.getKey()) == 0;
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFromBuffer, null,
				iterator, checkKeys);
		if (emptyIterator != null) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			iterator.close();
			return emptyIterator;
		}

		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev()
						&& checkKeys.checkKeys(hashKey, null, null, keyBytesFromBuffer, null, iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuffer, ByteBuffer> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, null, null, keyBytesFromBuffer, null, peekNext)) {
							return true;
						} else {
							fixFirst = false;
						}
					}
				}
				return false;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return RawTableRow.valueOfBuffer(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (useFixFirst()) {
					throw new UnsupportedOperationException("Not possible on first result for now...");
				}
				iterator.remove();
			}

			@Override
			public void close() {
				keyBytesFrom.release(keyBytesFrom.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	private TableIterator<H, R, V> reverseSeekToLast(final H hashKey, final R fromRangeKey, final R toRangeKey,
			final ByteBuffer keyBytesFrom, final ByteBuffer keyBytesTo, final DBIterator iterator,
			final CheckKeysFunction<H, R, V> checkKeys) {
		iterator.seek(keyBytesFrom);
		Entry<ByteBuffer, ByteBuffer> last = null;
		while (iterator.hasNext() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				iterator.peekNext())) {
			last = iterator.next();
		}
		// if there is no last one, there is nothing at all in the table
		if (last == null) {
			return new TableIterator<H, R, V>() {

				@Override
				public boolean hasNext() {
					return false;
				}

				@Override
				public TableRow<H, R, V> next() {
					throw new NoSuchElementException();
				}

				@Override
				public void remove() {
					throw new NoSuchElementException();
				}

				@Override
				public void close() {
				}
			};
		}
		// since last has been found, seek again for that one
		iterator.seek(last.getKey());
		return null;
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey, keyBytesFrom, keyBytesTo,
				peek) -> {
			final ByteBuffer peekKey = peek.getKey();
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0 && (fromRangeKey1 == null
					|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0);
		};
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		iterator.seek(keyBytesFromBuffer);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLast = ByteBufAllocator.DEFAULT.directBuffer();
			Util.combineBuf(keyBytesFromForSeekLast, hashKeySerde, rangeKeySerde, hashKey, null);
			final ByteBuffer keyBytesFromForSeekLastBuffer = keyBytesFromForSeekLast.nioBuffer();
			final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null,
					keyBytesFromForSeekLastBuffer, null, iterator, checkKeys);
			if (emptyIterator != null) {
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesFromForSeekLast.release(keyBytesFromForSeekLast.refCnt());
				iterator.close();
				return emptyIterator;
			}
		}
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev() && checkKeys.checkKeys(hashKey, fromRangeKey, null, keyBytesFromBuffer, null,
						iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuffer, ByteBuffer> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, fromRangeKey, null, keyBytesFromBuffer, null, peekNext)) {
							return true;
						} else {
							fixFirst = false;
						}
					}
				}
				return false;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return RawTableRow.valueOfBuffer(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (useFixFirst()) {
					throw new UnsupportedOperationException("Not possible on first result for now...");
				}
				iterator.remove();
			}

			@Override
			public void close() {
				keyBytesFrom.release(keyBytesFrom.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey1, keyBytesFrom, keyBytesTo,
				peek) -> {
			final ByteBuffer peekKey = peek.getKey();
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0
					&& (fromRangeKey1 == null
							|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0)
					&& (toRangeKey1 == null
							|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, peekKey) <= 0);
		};
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final ByteBuf keyBytesTo = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesToBuffer = keyBytesTo.nioBuffer();
		iterator.seek(keyBytesFromBuffer);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLast = ByteBufAllocator.DEFAULT.directBuffer();
			Util.combineBuf(keyBytesFromForSeekLast, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
			final ByteBuffer keyBytesFromForSeekLastBuffer = keyBytesFromForSeekLast.nioBuffer();
			final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey,
					keyBytesFromForSeekLastBuffer, keyBytesToBuffer, iterator, checkKeys);
			if (emptyIterator != null) {
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
				keyBytesFromForSeekLast.release(keyBytesFromForSeekLast.refCnt());
				iterator.close();
				return emptyIterator;
			}
		}
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFromBuffer,
						keyBytesToBuffer, iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuffer, ByteBuffer> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFromBuffer, keyBytesToBuffer,
								peekNext)) {
							return true;
						} else {
							fixFirst = false;
						}
					}
				}
				return false;
			}

			@Override
			public TableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return RawTableRow.valueOfBuffer(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawTableRow.valueOfBuffer(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (useFixFirst()) {
					throw new UnsupportedOperationException("Not possible on first result for now...");
				}
				iterator.remove();
			}

			@Override
			public void close() {
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
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
		delete(hashKey, null);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(buffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
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

	private static class AutoClosingTableIterator<_H, _R, _V> implements TableIterator<_H, _R, _V> {

		private final TableIterator<_H, _R, _V> delegate;
		private boolean closed;

		public AutoClosingTableIterator(final TableIterator<_H, _R, _V> delegate) {
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
		public TableRow<_H, _R, _V> next() {
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
	public TableRow<H, R, V> getLatest(final H hashKey) {
		final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey);
		try {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				final TableIterator<H, R, V> range = range(hashKey);
				try {
					if (range.hasNext()) {
						return range.next();
					} else {
						return null;
					}
				} finally {
					range.close();
				}
			}
		} finally {
			rangeReverse.close();
		}
	}

	@Override
	public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
		try {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				final TableIterator<H, R, V> range = range(hashKey, rangeKey);
				try {
					if (range.hasNext()) {
						return range.next();
					} else {
						return null;
					}
				} finally {
					range.close();
				}
			}
		} finally {
			rangeReverse.close();
		}
	}

	@Override
	public TableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, rangeKey);
		try {
			if (range.hasNext()) {
				return range.next();
			} else {
				return null;
			}
		} finally {
			range.close();
		}
	}

	@Override
	public TableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
		final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
		try {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				return null;
			}
		} finally {
			rangeReverse.close();
		}
	}

	@Override
	public Batch<H, V> newBatch() {
		return newRangeBatch();
	}

	@Override
	public RangeBatch<H, R, V> newRangeBatch() {
		return new EzLmDbBatch<H, R, V>(env, db, hashKeySerde, rangeKeySerde, valueSerde);
	}

	@Override
	public void deleteRange(final H hashKey) {
		final TableIterator<H, R, V> range = range(hashKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, fromRangeKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, fromRangeKey, toRangeKey);
		internalDeleteRange(range);
	}

	private void internalDeleteRange(final TableIterator<H, R, V> range) {
		final RangeBatch<H, R, V> batch = newRangeBatch();
		try {
			while (range.hasNext()) {
				final TableRow<H, R, V> next = range.next();
				batch.delete(next.getHashKey(), next.getRangeKey());
			}
			batch.flush();
		} finally {
			try {
				batch.close();
			} catch (final IOException e) {
				throw new DbException(e);
			}
			range.close();
		}
	}

	@FunctionalInterface
	private interface CheckKeysFunction<H, R, V> {
		boolean checkKeys(final H hashKey, final R fromRangeKey, final R toRangeKey, final ByteBuffer keyBytesFrom,
				final ByteBuffer keyBytesTo, final Entry<ByteBuffer, ByteBuffer> peek);
	}

}
