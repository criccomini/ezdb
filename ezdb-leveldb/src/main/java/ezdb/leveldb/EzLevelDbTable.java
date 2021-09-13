package ezdb.leveldb;

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
import ezdb.EmptyTableIterator;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.leveldb.util.EzLevelDBIterator;
import ezdb.leveldb.util.Slices;
import ezdb.serde.Serde;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLevelDbTable<H, R, V> implements RangeTable<H, R, V> {

	private static final WriteOptions DEFAULT_WRITE_OPTIONS = new WriteOptions();

	private final ReadOptions defaultReadOptions;

	private final ExtendedDbImpl db;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public EzLevelDbTable(final File path, final EzLevelDbJavaFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;

		final Options options = new Options();
		options.createIfMissing(true);
		this.defaultReadOptions = new ReadOptions().verifyChecksums(factory.isVerifyChecksums());
		options.comparator(new EzLevelDbJavaComparator(hashKeyComparator, rangeKeyComparator));

		try {
			this.db = factory.open(path, options);
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
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		final ByteBuf valueBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
		valueSerde.toBuffer(valueBuffer, value);
		try {
			db.put(Slices.wrapCopy(keyBuffer), Slices.wrapCopy(valueBuffer), DEFAULT_WRITE_OPTIONS);
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
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
		try {
			Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
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
	public TableIterator<H, R, V> range(final H hashKey) {
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom,
						Slices.unwrap(iterator.peekNextKey())) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
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
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom,
						Slices.unwrap(iterator.peekNextKey())) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
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
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuf keyBytesToBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesToBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesTo = keyBytesToBuf.nioBuffer();
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo,
						Slices.unwrap(iterator.peekNextKey())) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
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
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				peekKey) -> Util.compareKeys(hashKeyComparator, null, keyBytesFrom, Slices.unwrap(peekKey)) == 0;
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null, keyBytesFromBuf,
				keyBytesFrom, null, iterator, checkKeys);
		if (emptyIterator != null) {
			keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
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
						&& checkKeys.checkKeys(hashKey, null, null, keyBytesFrom, null, iterator.peekPrevKey());
			}

			private boolean useFixFirst() {
				if (fixFirst && iterator.hasNext()) {
					final Slice peekNextKey = iterator.peekNextKey();
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
			public TableRow<H, R, V> next() {
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

	private TableIterator<H, R, V> reverseSeekToLast(final H hashKey, final R fromRangeKey, final R toRangeKey,
			final ByteBuf keyBytesFromBuf, final ByteBuffer keyBytesFrom, final ByteBuffer keyBytesTo,
			final EzLevelDBIterator<H, R, V> iterator, final CheckKeysFunction<H, R, V> checkKeys) {
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		Slice lastKey = null;
		while (iterator.hasNext() && checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
				iterator.peekNextKey())) {
			lastKey = iterator.nextKey();
		}
		// if there is no last one, there is nothing at all in the table
		if (lastKey == null) {
			return EmptyTableIterator.get();
		}
		// since last has been found, seek again for that one
		iterator.seek(lastKey);
		return null;
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey, keyBytesFrom, keyBytesTo,
				peekKey) -> {
			final ByteBuffer peekKeyBuffer = Slices.unwrap(peekKey);
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKeyBuffer) == 0 && (fromRangeKey1 == null
					|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKeyBuffer) >= 0);
		};
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLastBuf = ByteBufAllocator.DEFAULT.heapBuffer();
			try {
				Util.combineBuf(keyBytesFromForSeekLastBuf, hashKeySerde, rangeKeySerde, hashKey, null);
				final ByteBuffer keyBytesFromForSeekLast = keyBytesFromForSeekLastBuf.nioBuffer();
				final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, null,
						keyBytesFromForSeekLastBuf, keyBytesFromForSeekLast, null, iterator, checkKeys);
				if (emptyIterator != null) {
					keyBytesFromBuf.release(keyBytesFromBuf.refCnt());
					iterator.close();
					return emptyIterator;
				}
			} finally {
				keyBytesFromForSeekLastBuf.release(keyBytesFromForSeekLastBuf.refCnt());
			}
		}
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

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
					final Slice peekNext = iterator.peekNextKey();
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
			public TableRow<H, R, V> next() {
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
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final EzLevelDBIterator<H, R, V> iterator = new EzLevelDBIterator<H, R, V>(
				db.extendedIterator(defaultReadOptions), hashKeySerde, rangeKeySerde, valueSerde);
		final CheckKeysFunction<H, R, V> checkKeys = (hashKey1, fromRangeKey1, toRangeKey1, keyBytesFrom, keyBytesTo,
				peekKey) -> {
			final ByteBuffer peekKeyBuffer = Slices.unwrap(peekKey);
			return Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekKeyBuffer) == 0 && (fromRangeKey1 == null
					|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, peekKeyBuffer) >= 0)
					&& (toRangeKey1 == null
							|| Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, peekKeyBuffer) <= 0);
		};
		final ByteBuf keyBytesFromBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFromBuf, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFrom = keyBytesFromBuf.nioBuffer();
		final ByteBuf keyBytesToBuf = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesToBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesTo = keyBytesToBuf.nioBuffer();
		iterator.seek(Slices.wrap(keyBytesFromBuf));
		if (!iterator.hasNext() || fromRangeKey == null) {
			final ByteBuf keyBytesFromForSeekLastBuf = ByteBufAllocator.DEFAULT.heapBuffer();
			try {
				Util.combineBuf(keyBytesFromForSeekLastBuf, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
				final ByteBuffer keyBytesFromForSeekLast = keyBytesFromForSeekLastBuf.nioBuffer();
				final TableIterator<H, R, V> emptyIterator = reverseSeekToLast(hashKey, null, toRangeKey,
						keyBytesFromForSeekLastBuf, keyBytesFromForSeekLast, keyBytesTo, iterator, checkKeys);
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
		return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

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
					final Slice peekNextKey = iterator.peekNextKey();
					if (peekNextKey != null) {
						if (checkKeys.checkKeys(hashKey, fromRangeKey, toRangeKey, keyBytesFrom, keyBytesTo,
								peekNextKey)) {
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
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(buffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		try {
			this.db.delete(Slices.wrapCopy(buffer), DEFAULT_WRITE_OPTIONS);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public void close() {
		try {
			this.db.close();
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
		return new EzLevelDbJavaBatch<H, R, V>(db, hashKeySerde, rangeKeySerde, valueSerde);
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
				final ByteBuffer keyBytesTo, final Slice peekKey);
	}

}
