package ezdb.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import ezdb.lmdb.util.LmDBJniDBIterator;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLmDbTable<H, R, V> implements RangeTable<H, R, V> {
	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<byte[]> hashKeyComparator;
	private final Comparator<byte[]> rangeKeyComparator;

	public EzLmDbTable(File path, EzLmDbFactory factory, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde, Comparator<byte[]> hashKeyComparator, Comparator<byte[]> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;

		try {
			this.env = factory.create(path, EnvFlags.MDB_NOTLS);
		} catch (IOException e) {
			throw new DbException(e);
		}
		EzLmDbComparator comparator = new EzLmDbComparator(hashKeyComparator, rangeKeyComparator);
		try {
			this.db = factory.open(env, comparator, DbiFlags.MDB_CREATE);
		} catch (IOException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void put(H hashKey, V value) {
		put(hashKey, null, value);
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		Txn<ByteBuffer> txn = env.txnWrite();
		try {
			db.put(txn, ByteBuffer.wrap(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)), ByteBuffer.wrap(valueSerde.toBytes(value)));
		} finally {
			txn.commit();
			txn.close();
		}
	}

	@Override
	public V get(H hashKey) {
		return get(hashKey, null);
	}

	@Override
	public V get(H hashKey, R rangeKey) {
		Txn<ByteBuffer> txn = env.txnRead();
		try {
			ByteBuffer valueBytes = db.get(txn, ByteBuffer.wrap(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)));
	
			if (valueBytes == null) {
				return null;
			}
	
			return valueSerde.fromBytes(valueBytes.array());
		} finally {
			txn.close();
		}
	}

	@Override
	public TableIterator<H, R, V> range(H hashKey) {
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public void close() {
				try {
					iterator.close();
				} catch (Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext()
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public void close() {
				try {
					iterator.close();
				} catch (Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		iterator.seek(keyBytesFrom);
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo,
						iterator.peekNext().getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
			}

			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public void close() {
				try {
					iterator.close();
				} catch (Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0;
			}
		};
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
		TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFrom, null, iterator,
				checkKeys);
		if (emptyIterator != null) {
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
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
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
					return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	private TableIterator<H, R, V> reverseSeekToLast(H hashKey, R fromRangeKey, R toRangeKey, final byte[] keyBytesFrom,
			byte[] keyBytesTo, final DBIterator iterator, Function<CheckKeysRequest, Boolean> checkKeys) {
		iterator.seek(keyBytesFrom);
		Entry<byte[], byte[]> last = null;
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

	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0
						&& (input.getFromRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesFrom(), input.getPeekKey()) >= 0);
			}
		};
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom);
		if (!iterator.hasNext() || fromRangeKey == null) {
			byte[] keyBytesFromForSeekLast = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
			TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFromForSeekLast, null,
					iterator, checkKeys);
			if (emptyIterator != null) {
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
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
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
					return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final DBIterator iterator = new LmDBJniDBIterator(db.newIterator());
		final Function<CheckKeysRequest, Boolean> checkKeys = new Function<CheckKeysRequest, Boolean>() {
			@Override
			public Boolean apply(CheckKeysRequest input) {
				return Util.compareKeys(hashKeyComparator, null, input.getKeyBytesFrom(), input.getPeekKey()) == 0
						&& (input.getFromRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesFrom(), input.getPeekKey()) >= 0)
						&& (input.getToRangeKey() == null || Util.compareKeys(hashKeyComparator, rangeKeyComparator,
								input.getKeyBytesTo(), input.getPeekKey()) <= 0);
			}
		};
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		iterator.seek(keyBytesFrom);
		if (!iterator.hasNext() || fromRangeKey == null) {
			byte[] keyBytesFromForSeekLast = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
			TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey, keyBytesFromForSeekLast,
					keyBytesTo, iterator, checkKeys);
			if (emptyIterator != null) {
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
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
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
					return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	@Override
	public void delete(H hashKey) {
		delete(hashKey, null);
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		Txn<ByteBuffer> txn = env.txnRead();
		try {
			this.db.delete(txn, ByteBuffer.wrap(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)));
		} finally {
			txn.commit();
			txn.close();
		}
	}

	@Override
	public void close() {
		try {
			this.db.close();
			this.env.close();
		} catch (Exception e) {
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

		private final byte[] keyBytesFrom;
		private final byte[] keyBytesTo;
		private final Entry<byte[], byte[]> peek;
		private final H hashKey;
		private final R fromRangeKey;
		private final R toRangeKey;

		public CheckKeysRequest(H hashKey, R fromRangeKey, R toRangeKey, byte[] keyBytesFrom, byte[] keyBytesTo,
				Entry<byte[], byte[]> peek) {
			this.hashKey = hashKey;
			this.fromRangeKey = fromRangeKey;
			this.toRangeKey = toRangeKey;
			this.keyBytesFrom = keyBytesFrom;
			this.keyBytesTo = keyBytesTo;
			this.peek = peek;
		}

		public byte[] getKeyBytesFrom() {
			return keyBytesFrom;
		}

		public byte[] getKeyBytesTo() {
			return keyBytesTo;
		}

		public byte[] getPeekKey() {
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
					+ new RawTableRow<H, R, V>(peek, hashKeySerde, rangeKeySerde, valueSerde).toString();
		}

	}

	@Override
	public TableRow<H, R, V> getLatest(H hashKey) {
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
	public TableRow<H, R, V> getLatest(H hashKey, R rangeKey) {
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
	public TableRow<H, R, V> getNext(H hashKey, R rangeKey) {
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
	public TableRow<H, R, V> getPrev(H hashKey, R rangeKey) {
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
		return new EzLmDbBatch<H, R, V>(db, hashKeySerde, rangeKeySerde, valueSerde);
	}

	@Override
	public void deleteRange(H hashKey) {
		TableIterator<H, R, V> range = range(hashKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(H hashKey, R fromRangeKey) {
		TableIterator<H, R, V> range = range(hashKey, fromRangeKey);
		internalDeleteRange(range);
	}

	@Override
	public void deleteRange(H hashKey, R fromRangeKey, R toRangeKey) {
		TableIterator<H, R, V> range = range(hashKey, fromRangeKey, toRangeKey);
		internalDeleteRange(range);
	}

	private void internalDeleteRange(TableIterator<H, R, V> range) {
		RangeBatch<H, R, V> batch = newRangeBatch();
		try {
			while (range.hasNext()) {
				TableRow<H, R, V> next = range.next();
				batch.delete(next.getHashKey(), next.getRangeKey());
			}
			batch.flush();
		} finally {
			try {
				batch.close();
			} catch (IOException e) {
				throw new DbException(e);
			}
		}
	}

}
