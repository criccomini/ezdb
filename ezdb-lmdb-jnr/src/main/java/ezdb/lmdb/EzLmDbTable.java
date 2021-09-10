package ezdb.lmdb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.function.Function;

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
	private final Env<ByteBuf> env;
	private final Dbi<ByteBuf> db;
	private final ByteBufAllocator allocator;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public EzLmDbTable(final File path, final EzLmDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.allocator = factory.getAllocator();

		try {
			this.env = factory.create(path.getParentFile(), EnvFlags.MDB_NOTLS);
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
		final ByteBuf keyBuf = allocator.buffer();
		final ByteBuf valueBuf = allocator.buffer();
		try {
			Util.combine(keyBuf, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			valueSerde.toBuffer(valueBuf, value);
			db.put(keyBuf, valueBuf);
		} finally {
			keyBuf.release(keyBuf.refCnt());
			valueBuf.release(valueBuf.refCnt());
		}
	}

	@Override
	public V get(final H hashKey) {
		return get(hashKey, null);
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final ByteBuf buf = allocator.buffer();
		final Txn<ByteBuf> txn = env.txnRead();
		try {
			Util.combine(buf, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			final ByteBuf valueBytes = db.get(txn, buf);

			if (valueBytes == null) {
				return null;
			}
			try {
				return valueSerde.fromBuffer(valueBytes);
			} finally {
				valueBytes.release();
			}
		} finally {
			txn.close();
			buf.release(buf.refCnt());
		}
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final DBIterator iterator = new LmDBJnrDBIterator(env, db);
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
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
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
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
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		final ByteBuf keyBytesTo = keyBytesFrom.clear();
		Util.combine(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo,
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
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(final CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0;
			}
		};
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFrom, null,
				iterator, checkKeys);
		if (emptyIterator != null) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			iterator.close();
			return emptyIterator;
		}

		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

			private boolean fixFirst = true;;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev() && checkKeys
						.apply(new CheckKeysRequest(hashKey, null, null, keyBytesFrom, null, iterator.peekPrev()));
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuf, ByteBuf> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.apply(new CheckKeysRequest(hashKey, null, null, keyBytesFrom, null, peekNext))) {
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
			final ByteBuf keyBytesFrom, final ByteBuf keyBytesTo, final DBIterator iterator,
			final Function<CheckKeysRequest, Boolean> checkKeys) {
		iterator.seek(keyBytesFrom);
		Entry<ByteBuf, ByteBuf> last = null;
		while (iterator.hasNext() && checkKeys.apply(new CheckKeysRequest(hashKey, fromRangeKey, toRangeKey,
				keyBytesFrom, keyBytesTo, iterator.peekNext()))) {
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
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(final CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0
						&& (input.getFromRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesFrom(), input.getPeekKey()) >= 0);
			}
		};
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLast = keyBytesFrom.clear();
			Util.combine(keyBytesFromForSeekLast, hashKeySerde, rangeKeySerde, hashKey, null);
			final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFromForSeekLast,
					null, iterator, checkKeys);
			if (emptyIterator != null) {
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
				return iterator.hasPrev() && checkKeys.apply(
						new CheckKeysRequest(hashKey, fromRangeKey, null, keyBytesFrom, null, iterator.peekPrev()));
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuf, ByteBuf> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.apply(
								new CheckKeysRequest(hashKey, fromRangeKey, null, keyBytesFrom, null, peekNext))) {
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
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(final CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0
						&& (input.getFromRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesFrom(), input.getPeekKey()) >= 0)
						&& (input.getToRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesTo(), input.getPeekKey()) <= 0);
			}
		};
		final ByteBuf keyBytesFrom = allocator.buffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		final ByteBuf keyBytesTo = allocator.buffer();
		Util.combine(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLast = keyBytesFrom.clear();
			Util.combine(keyBytesFromForSeekLast, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
			final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey,
					keyBytesFromForSeekLast, keyBytesTo, iterator, checkKeys);
			if (emptyIterator != null) {
				keyBytesFromForSeekLast.release(keyBytesFromForSeekLast.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
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
				return iterator.hasPrev() && checkKeys.apply(new CheckKeysRequest(hashKey, fromRangeKey, toRangeKey,
						keyBytesFrom, keyBytesTo, iterator.peekPrev()));
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<ByteBuf, ByteBuf> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.apply(new CheckKeysRequest(hashKey, fromRangeKey, toRangeKey, keyBytesFrom,
								keyBytesTo, peekNext))) {
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
		final ByteBuf buf = allocator.buffer();
		try {
			Util.combine(buf, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			this.db.delete(buf);
		} finally {
			buf.release(buf.refCnt());
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

	private class CheckKeysRequest {

		private final ByteBuf keyBytesFrom;
		private final ByteBuf keyBytesTo;
		private final Entry<ByteBuf, ByteBuf> peek;
		private final H hashKey;
		private final R fromRangeKey;
		private final R toRangeKey;

		public CheckKeysRequest(final H hashKey, final R fromRangeKey, final R toRangeKey, final ByteBuf keyBytesFrom,
				final ByteBuf keyBytesTo, final Entry<ByteBuf, ByteBuf> peek) {
			this.hashKey = hashKey;
			this.fromRangeKey = fromRangeKey;
			this.toRangeKey = toRangeKey;
			this.keyBytesFrom = keyBytesFrom;
			this.keyBytesTo = keyBytesTo;
			this.peek = peek;
		}

		public ByteBuf getKeyBytesFrom() {
			return keyBytesFrom;
		}

		public ByteBuf getKeyBytesTo() {
			return keyBytesTo;
		}

		public ByteBuf getPeekKey() {
			return peek.getKey();
		}

		public H getHashKey() {
			return hashKey;
		}

		public R getFromRangeKey() {
			return fromRangeKey;
		}

		public R getToRangeKey() {
			return toRangeKey;
		}

		@Override
		public String toString() {
			return "CheckKeysRequest [hashKey=" + getHashKey() + ", fromRangeKey=" + getFromRangeKey() + ", toRangeKey="
					+ getToRangeKey() + "] -> "
					+ RawTableRow.valueOfBuffer(peek, hashKeySerde, rangeKeySerde, valueSerde).toString();
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
		return new EzLmDbBatch<H, R, V>(allocator, env, db, hashKeySerde, rangeKeySerde, valueSerde);
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
		}
	}

}
