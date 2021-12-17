package ezdb.leveldb.table.range;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import ezdb.DbException;
import ezdb.leveldb.EzLevelDbJniFactory;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.RangeTableRow;
import ezdb.table.range.EmptyRangeTableIterator;
import ezdb.table.range.RangeBatch;
import ezdb.table.range.RangeTable;
import ezdb.table.range.RawRangeTableRow;
import ezdb.util.TableIterator;
import ezdb.util.Util;

public class EzLevelDbJniRangeTable<H, R, V> implements RangeTable<H, R, V> {
	private final DB db;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzLevelDbJniRangeTable(final File path, final EzLevelDbJniFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;

		final Options options = new Options();
		options.createIfMissing(true);
		options.comparator(new EzLevelDbJniRangeComparator(hashKeyComparator, rangeKeyComparator));

		try {
			this.db = factory.open(path, options, true);
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
		db.put(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
	}

	@Override
	public V get(final H hashKey) {
		return get(hashKey, null);
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final byte[] valueBytes = db.get(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey));

		if (valueBytes == null) {
			return null;
		}

		return valueSerde.fromBytes(valueBytes);
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> range() {
		final DBIterator iterator = db.iterator();
		iterator.seekToFirst();
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey) {
		if (hashKey == null) {
			return range();
		}
		final DBIterator iterator = db.iterator();
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, null);
		iterator.seek(keyBytesFrom.array());
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom,
						ByteBuffer.wrap(iterator.peekNext().getKey())) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final DBIterator iterator = db.iterator();
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom.array());
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom,
						ByteBuffer.wrap(iterator.peekNext().getKey())) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final DBIterator iterator = db.iterator();
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesTo = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		iterator.seek(keyBytesFrom.array());
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo,
						ByteBuffer.wrap(iterator.peekNext().getKey())) >= 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
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
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse() {
		final DBIterator iterator = db.iterator();
		iterator.seekToLast();
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<RangeTableRow<H, R, V>>() {

			private boolean fixFirst = true;

			@Override
			public boolean hasNext() {
				if (useFixFirst()) {
					return true;
				}
				return iterator.hasPrev();
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
					if (peekNext != null) {
						return true;
					}
				}
				return false;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				if (useFixFirst()) {
					fixFirst = false;
					return RawRangeTableRow.valueOfBytes(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey) {
		if (hashKey == null) {
			return rangeReverse();
		}
		final DBIterator iterator = db.iterator();
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				peek) -> Util.compareKeys(hashKeyComparator, null, keyBytesFrom, ByteBuffer.wrap(peek.getKey())) == 0;
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, null);
		final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFrom,
				null, iterator, checkKeys);
		if (emptyIterator != null) {
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
						&& checkKeys.checkKeys(hashKey, null, null, keyBytesFrom, null, iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
					if (peekNext != null) {
						if (checkKeys.checkKeys(hashKey, null, null, keyBytesFrom, null, peekNext)) {
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
					return RawRangeTableRow.valueOfBytes(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
				try {
					iterator.close();
				} catch (final Exception e) {
					throw new DbException(e);
				}
			}
		});
	}

	private TableIterator<RangeTableRow<H, R, V>> reverseSeekToLast(final H hashKey, final R fromRangeKey,
			final R toRangeKey, final ByteBuffer keyBytesFrom, final ByteBuffer keyBytesTo, final DBIterator iterator,
			final CheckKeysFunction<H, R, V> checkKeys) {
		iterator.seek(keyBytesFrom.array());
		Entry<byte[], byte[]> last = null;
		while (iterator.hasNext() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				iterator.peekNext())) {
			last = iterator.next();
		}
		// if there is no last one, there is nothing at all in the table
		if (last == null) {
			return EmptyRangeTableIterator.get();
		}
		// since last has been found, seek again for that one
		iterator.seek(last.getKey());
		return null;
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final DBIterator iterator = db.iterator();
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey, keyBytesFrom, keyBytesTo,
				peek) -> {
			final ByteBuffer peekKey = ByteBuffer.wrap(peek.getKey());
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0 && (fromRangeKey1 == null
					|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0);
		};
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		iterator.seek(keyBytesFrom.array());
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuffer keyBytesFromForSeekLast = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, null);
			final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, null,
					keyBytesFromForSeekLast, null, iterator, checkKeys);
			if (emptyIterator != null) {
				iterator.close();
				return emptyIterator;
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
						&& checkKeys.checkKeys(hashKey, fromRangeKey, null, keyBytesFrom, null, iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
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
					return RawRangeTableRow.valueOfBytes(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
		final DBIterator iterator = db.iterator();
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey1, keyBytesFrom, keyBytesTo,
				peek) -> {
			final ByteBuffer peekKey = ByteBuffer.wrap(peek.getKey());
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKey) == 0
					&& (fromRangeKey1 == null
							|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKey) >= 0)
					&& (toRangeKey1 == null
							|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, peekKey) <= 0);
		};
		final ByteBuffer keyBytesFrom = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesTo = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		iterator.seek(keyBytesFrom.array());
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuffer keyBytesFromForSeekLast = Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey,
					toRangeKey);
			final TableIterator<RangeTableRow<H, R, V>> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey,
					keyBytesFromForSeekLast, keyBytesTo, iterator, checkKeys);
			if (emptyIterator != null) {
				iterator.close();
				return emptyIterator;
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
						keyBytesTo, iterator.peekPrev());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Entry<byte[], byte[]> peekNext = iterator.peekNext();
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
					return RawRangeTableRow.valueOfBytes(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
				}
				if (hasNext()) {
					return RawRangeTableRow.valueOfBytes(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
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
		this.db.delete(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
	}

	@Override
	public void close() {
		try {
			this.db.close();
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
		return new EzLevelDbJniRangeBatch<H, R, V>(db, hashKeySerde, rangeKeySerde, valueSerde);
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
				final ByteBuffer keyBytesTo, final Entry<byte[], byte[]> peek);
	}

}
