package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.Store.Entry;

import ezdb.EmptyTableIterator;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.ObjectTableKey;
import ezdb.util.ObjectTableRow;
import ezdb.util.Util;

public class LsmTreeTable<H, R, V> implements RangeTable<H, R, V> {
	private final Store<ObjectTableKey<H, R>, V> store;
	private final Comparator<H> hashKeyComparator;
	private final Comparator<R> rangeKeyComparator;
	private final EzLsmTreeDbComparator<H, R> keyComparator;

	public LsmTreeTable(final File path, final EzLsmTreeDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<H> hashKeyComparator,
			final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.keyComparator = new EzLsmTreeDbComparator<H, R>(hashKeyComparator, rangeKeyComparator);
		try {
			this.store = factory.open(path,
					new ObjectTableKeySerializer<H, R>(hashKeySerde, rangeKeySerde, keyComparator),
					EzdbSerializer.valueOf(valueSerde), keyComparator);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		put(hashKey, null, value);
	}

	@Override
	public V get(final H hashKey) {
		return get(hashKey, null);
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		try {
			store.put(Util.combine(hashKey, rangeKey, keyComparator), value);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		try {
			final V value = store.get(Util.combine(hashKey, rangeKey, keyComparator));
			return value;
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			if (hashKey == null) {
				iterator = store.iterator();
			} else {
				iterator = store.iterator(keyBytesFrom, true);
			}
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				TableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final ObjectTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey, keyComparator);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) > 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, next.getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public void delete(final H hashKey) {
		delete(hashKey, null);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		try {
			store.delete(Util.combine(hashKey, rangeKey, keyComparator));
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		try {
			store.close();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			if (hashKey == null) {
				iterator = store.reverseIterator();
			} else {
				final ObjectTableKey<H, R> seekLast = seekLastFrom(keyBytesFrom);
				if (seekLast == null) {
					return EmptyTableIterator.get();
				}
				iterator = store.reverseIterator(seekLast, true);
			}
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				TableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	private ObjectTableKey<H, R> seekLastFrom(final ObjectTableKey<H, R> keyBytesFrom) {
		TableRow<H, R, V> last = null;
		final Iterator<TableRow<H, R, V>> iterator = range(keyBytesFrom.getHashKey(), keyBytesFrom.getRangeKey());
		try {
			while (true) {
				last = iterator.next();
			}
		} catch (final NoSuchElementException e) {
			// end reached
		}
		if (last == null) {
			return null;
		} else {
			return new ObjectTableKey<H, R>(last.getHashKey(), last.getRangeKey(), keyComparator);
		}
	}

	private ObjectTableKey<H, R> seekLastTo(final ObjectTableKey<H, R> keyBytesTo) {
		try {
			final Entry<ObjectTableKey<H, R>, V> floor = store.floor(keyBytesTo);
			if (floor == null) {
				return null;
			}
			return floor.getKey();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			final ObjectTableKey<H, R> seekLast = seekLastTo(keyBytesFrom);
			if (seekLast == null) {
				return EmptyTableIterator.get();
			}
			iterator = store.reverseIterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final ObjectTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey, keyComparator);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) < 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Store.Entry<ObjectTableKey<H, R>, V>> iterator;
		try {
			final ObjectTableKey<H, R> seekLast;
			if (fromRangeKey == null) {
				seekLast = seekLastFrom(keyBytesTo);
			} else {
				seekLast = seekLastTo(keyBytesFrom);
			}
			if (seekLast == null) {
				return EmptyTableIterator.get();
			}
			iterator = store.reverseIterator(seekLast, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<H, R, V>() {
			private Store.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, next.getKey(), keyBytesTo) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next.getKey(), next.getValue());
				}

				if (iterator.hasNext()) {
					next = iterator.next();
				} else {
					next = null;
				}

				if (row != null) {
					return row;
				} else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				if (next == null) {
					throw new IllegalStateException("next is null");
				}
				try {
					store.delete(next.getKey());
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public TableRow<H, R, V> getLatest(final H hashKey) {
		try (TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey)) {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				return null;
			}
		}
	}

	@Override
	public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		try {
			final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, rangeKey, keyComparator);
			Entry<ObjectTableKey<H, R>, V> value = store.floor(keyBytesFrom);
			if (value == null || Util.compareKeys(hashKeyComparator, null, keyBytesFrom, value.getKey()) != 0) {
				value = store.ceil(keyBytesFrom);
			}
			if (value != null) {
				return new ObjectTableRow<H, R, V>(value.getKey(), value.getValue());
			} else {
				return null;
			}
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, rangeKey);
		if (range.hasNext()) {
			return range.next();
		}
		return null;
	}

	@Override
	public TableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		} else {
			final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			}
			return null;
		}
	}

	@Override
	public Batch<H, V> newBatch() {
		return new Batch<H, V>() {

			@Override
			public void close() throws IOException {
			}

			@Override
			public void put(final H hashKey, final V value) {
				LsmTreeTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				LsmTreeTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}
		};
	}

	@Override
	public RangeBatch<H, R, V> newRangeBatch() {
		return new RangeBatch<H, R, V>() {

			@Override
			public void close() throws IOException {
			}

			@Override
			public void put(final H hashKey, final V value) {
				LsmTreeTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				LsmTreeTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(final H hashKey, final R rangeKey, final V value) {
				LsmTreeTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(final H hashKey, final R rangeKey) {
				LsmTreeTable.this.delete(hashKey, rangeKey);
			}
		};
	}

	@Override
	public void deleteRange(final H hashKey) {
		try (TableIterator<H, R, V> range = range(hashKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey) {
		try (TableIterator<H, R, V> range = range(hashKey, fromRangeKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		try (TableIterator<H, R, V> range = range(hashKey, fromRangeKey, toRangeKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

}
