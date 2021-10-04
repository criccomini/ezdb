package ezdb.lmdb.table.range;

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
import ezdb.lmdb.util.EzDBRangeIterator;
import ezdb.lmdb.util.LmDBJnrDBRangeIterator;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.RangeTableRow;
import ezdb.table.range.EmptyRangeTableIterator;
import ezdb.table.range.RangeBatch;
import ezdb.table.range.RangeTable;
import ezdb.util.TableIterator;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

public class EzLmDbRangeTable<H, R, V> implements RangeTable<H, R, V> {
	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzLmDbRangeTable(final File path, final EzLmDbFactory factory, final Serde<H> hashKeySerde,
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
		final EzLmDbRangeComparator comparator = new EzLmDbRangeComparator(hashKeyComparator, rangeKeyComparator);
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

			return valueSerde.fromBuffer(Unpooled.wrappedBuffer(valueBytes));
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
			txn.close();
		}
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey) {
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNextKey()) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNextKey()) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		final ByteBuf keyBytesToBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesToBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesTo = keyBytesToBuf.nioBuffer();
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo,
						iterator.peekNextKey()) >= 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				keyBytesToBuf.release(keyBytesToBuf.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey) {
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				peekKey) -> Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0;
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFrom,
				null, iterator, checkKeys);
		if (emptyIterator != null) {
			keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
			iterator.close();
			return emptyIterator;
		}

		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev()
						&& checkKeys.checkKeys(hashKey, null, null, keyBytesFrom, null, iterator.peekPrevKey());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final ByteBuffer peekNextKey = iterator.peekNextKey();
					if (peekNextKey != null) {
						if (checkKeys.checkKeys(hashKey, null, null, keyBytesFrom, null, peekNextKey)) {
							return true;
						} else {
							fixFirst = false;
						}
					}
				}
				return false;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return iterator.peekNext();
				}
				if (hasNext()) {
					return iterator.prev();
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	private TableIterator<RangeTableRow<H, R, V>> reverseSeekToLast(final H hashKey, final R fromRangeKey,
			final R toRangeKey, final ByteBuffer keyBytesFrom, final ByteBuffer keyBytesTo, final EzDBRangeIterator iterator,
			final CheckKeysFunction<H, R, V> checkKeys) {
		iterator.seek(keyBytesFrom);
		ByteBuffer last = null;
		while (iterator.hasNext() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				iterator.peekNextKey())) {
			last = iterator.nextKey();
		}
		// if there is no last one, there is nothing at all in the table
		if (last == null) {
			return EmptyRangeTableIterator.get();
		}
		// since last has been found, seek again for that one
		iterator.seek(last);
		return null;
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey, keyBytesFrom, keyBytesTo,
				peekKey) -> {
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0 && (fromRangeKey1 == null
					|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0);
		};
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(keyBytesFrom);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLastBuf = ByteBufAllocator.DEFAULT.directBuffer();
			try {
				Util.combineBuf(keyBytesFromForSeekLastBuf, hashKeySerde, rangeKeySerde, hashKey, null);
				final ByteBuffer keyBytesFromForSeekLast = keyBytesFromForSeekLastBuf.nioBuffer();
				final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, null,
						keyBytesFromForSeekLast, null, iterator, checkKeys);
				if (emptyIterator != null) {
					keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
					iterator.close();
					return emptyIterator;
				}
			} finally {
				keyBytesFromForSeekLastBuf.release(keyBytesFromForSeekLastBuf.refCnt());
			}
		}
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev()
						&& checkKeys.checkKeys(hashKey, fromRangeKey, null, keyBytesFrom, null, iterator.peekPrevKey());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final ByteBuffer peekNext = iterator.peekNextKey();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, fromRangeKey, null, keyBytesFrom, null, peekNext)) {
							return true;
						} else {
							fixFirst = false;
						}
					}
				}
				return false;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return iterator.peekNext();
				}
				if (hasNext()) {
					return iterator.prev();
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey,
			final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final EzDBRangeIterator<H, R, V> iterator = new LmDBJnrDBRangeIterator<H, R, V>(env, db, hashKeySerde, rangeKeySerde,
				valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey1, keyBytesFrom, keyBytesTo,
				peekKey) -> Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0
						&& (fromRangeKey1 == null
								|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0)
						&& (toRangeKey1 == null
								|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, peekKey) <= 0);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		final ByteBuf keyBytesToBuf = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBytesToBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesTo = keyBytesToBuf.nioBuffer();
		iterator.seek(keyBytesFrom);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLastBuf = ByteBufAllocator.DEFAULT.directBuffer();
			try {
				Util.combineBuf(keyBytesFromForSeekLastBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
				final ByteBuffer keyBytesFromForSeekLast = keyBytesFromForSeekLastBuf.nioBuffer();
				final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey,
						keyBytesFromForSeekLast, keyBytesTo, iterator, checkKeys);
				if (emptyIterator != null) {
					keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
					keyBytesToBuf.release(keyBytesToBuf.refCnt());
					iterator.close();
					return emptyIterator;
				}
			} finally {
				keyBytesFromForSeekLastBuf.release(keyBytesFromForSeekLastBuf.refCnt());
			}
		}
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom,
						keyBytesTo, iterator.peekPrevKey());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final ByteBuffer peekNext = iterator.peekNextKey();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
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
			public RangeTableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return iterator.peekNext();
				}
				if (hasNext()) {
					return iterator.prev();
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
				keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
				keyBytesToBuf.release(keyBytesToBuf.refCnt());
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

	private static class AutoClosingTableIterator<_H, _R, _V> implements TableIterator<RangeTableRow<_H, _R, _V>> {

		private final TableIterator<RangeTableRow<_H, _R, _V>> delegate;
		private boolean closed;

		public AutoClosingTableIterator(final TableIterator<RangeTableRow<_H, _R, _V>> delegate) {
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
		public RangeTableRow<_H, _R, _V> next() {
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
	public RangeTableRow<H, R, V> getLatest(final H hashKey) {
		final TableIterator<RangeTableRow<H, R, V>> rangeReverse = rangeReverse(hashKey);
		try {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey);
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
	public RangeTableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		final TableIterator<RangeTableRow<H, R, V>> rangeReverse = rangeReverse(hashKey, rangeKey);
		try {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, rangeKey);
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
	public RangeTableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
		final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, rangeKey);
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
	public RangeTableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
		final TableIterator<RangeTableRow<H, R, V>> rangeReverse = rangeReverse(hashKey, rangeKey);
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
		return new EzLmDbRangeBatch<H, R, V>(env, db, hashKeySerde, rangeKeySerde, valueSerde);
	}

	@Override
	public void deleteRange(final H hashKey) {
		final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey) {
		final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, fromRangeKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, fromRangeKey, toRangeKey);
		internalDeleteRange(range);
	}

	private void internalDeleteRange(final TableIterator<RangeTableRow<H, R, V>> range) {
		final RangeBatch<H, R, V> batch = newRangeBatch();
		try {
			while (range.hasNext()) {
				final RangeTableRow<H, R, V> next = range.next();
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
				final ByteBuffer keyBytesTo, final ByteBuffer peekKey);
	}

}
