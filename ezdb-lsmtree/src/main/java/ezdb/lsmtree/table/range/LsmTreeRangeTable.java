package ezdb.lsmtree.table.range;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.Store.Entry;

import ezdb.lsmtree.EzLsmTreeDbFactory;
import ezdb.lsmtree.EzdbSerializer;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.RangeTableRow;
import ezdb.table.range.EmptyRangeTableIterator;
import ezdb.table.range.RangeBatch;
import ezdb.table.range.RangeTable;
import ezdb.util.ObjectRangeTableKey;
import ezdb.util.ObjectRangeTableRow;
import ezdb.util.TableIterator;
import ezdb.util.Util;

public class LsmTreeRangeTable<H, R, V> implements RangeTable<H, R, V> {
	private final Store<ObjectRangeTableKey<H, R>, V> store;
	private final Comparator<H> hashKeyComparator;
	private final Comparator<R> rangeKeyComparator;
	private final EzLsmTreeDbRangeComparator<H, R> keyComparator;

	public LsmTreeRangeTable(final File path, final EzLsmTreeDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<H> hashKeyComparator,
			final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.keyComparator = new EzLsmTreeDbRangeComparator<H, R>(hashKeyComparator, rangeKeyComparator);
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
	public TableIterator<RangeTableRow<H, R, V>> range() {
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				RangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey) {
		if (hashKey == null) {
			return range();
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				RangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				ObjectRangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final ObjectRangeTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey, keyComparator);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) > 0) {
			return EmptyRangeTableIterator.get();
		}
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			iterator = store.iterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, next.getKey()) >= 0;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				ObjectRangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse() {
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			iterator = store.reverseIterator();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public RangeTableRow<H, R, V> next() {
				RangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey) {
		if (hashKey == null) {
			return rangeReverse();
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			final ObjectRangeTableKey<H, R> seekLast = seekLastFrom(keyBytesFrom);
			if (seekLast == null) {
				return EmptyRangeTableIterator.get();
			}
			iterator = store.reverseIterator(seekLast, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
			public RangeTableRow<H, R, V> next() {
				RangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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

	private ObjectRangeTableKey<H, R> seekLastFrom(final ObjectRangeTableKey<H, R> keyBytesFrom) {
		RangeTableRow<H, R, V> last = null;
		final TableIterator<RangeTableRow<H, R, V>> iterator = range(keyBytesFrom.getHashKey(),
				keyBytesFrom.getRangeKey());
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
			return new ObjectRangeTableKey<H, R>(last.getHashKey(), last.getRangeKey(), keyComparator);
		}
	}

	private ObjectRangeTableKey<H, R> seekLastTo(final ObjectRangeTableKey<H, R> keyBytesTo) {
		try {
			final Entry<ObjectRangeTableKey<H, R>, V> floor = store.floor(keyBytesTo);
			if (floor == null) {
				return null;
			}
			return floor.getKey();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			final ObjectRangeTableKey<H, R> seekLast = seekLastTo(keyBytesFrom);
			if (seekLast == null) {
				return EmptyRangeTableIterator.get();
			}
			iterator = store.reverseIterator(keyBytesFrom, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
			public RangeTableRow<H, R, V> next() {
				ObjectRangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public TableIterator<RangeTableRow<H, R, V>> rangeReverse(final H hashKey, final R fromRangeKey,
			final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final ObjectRangeTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey, keyComparator);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) < 0) {
			return EmptyRangeTableIterator.get();
		}
		final Iterator<Store.Entry<ObjectRangeTableKey<H, R>, V>> iterator;
		try {
			final ObjectRangeTableKey<H, R> seekLast;
			if (fromRangeKey == null) {
				seekLast = seekLastFrom(keyBytesTo);
			} else {
				seekLast = seekLastTo(keyBytesFrom);
			}
			if (seekLast == null) {
				return EmptyRangeTableIterator.get();
			}
			iterator = store.reverseIterator(seekLast, true);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<RangeTableRow<H, R, V>>() {
			private Store.Entry<ObjectRangeTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
			public RangeTableRow<H, R, V> next() {
				ObjectRangeTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectRangeTableRow<H, R, V>(next.getKey(), next.getValue());
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
	public RangeTableRow<H, R, V> getLatest(final H hashKey) {
		try (TableIterator<RangeTableRow<H, R, V>> rangeReverse = rangeReverse(hashKey)) {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				return null;
			}
		}
	}

	@Override
	public RangeTableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		try {
			final ObjectRangeTableKey<H, R> keyBytesFrom = Util.combine(hashKey, rangeKey, keyComparator);
			Entry<ObjectRangeTableKey<H, R>, V> value = store.floor(keyBytesFrom);
			if (value == null || Util.compareKeys(hashKeyComparator, null, keyBytesFrom, value.getKey()) != 0) {
				value = store.ceil(keyBytesFrom);
			}
			if (value == null || Util.compareKeys(hashKeyComparator, null, keyBytesFrom, value.getKey()) != 0) {
				return null;
			} else {
				return new ObjectRangeTableRow<H, R, V>(value.getKey(), value.getValue());
			}
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public RangeTableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
		final TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, rangeKey);
		if (range.hasNext()) {
			return range.next();
		}
		return null;
	}

	@Override
	public RangeTableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		} else {
			final TableIterator<RangeTableRow<H, R, V>> rangeReverse = rangeReverse(hashKey, rangeKey);
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
				LsmTreeRangeTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				LsmTreeRangeTable.this.delete(hashKey);
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
				LsmTreeRangeTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				LsmTreeRangeTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(final H hashKey, final R rangeKey, final V value) {
				LsmTreeRangeTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(final H hashKey, final R rangeKey) {
				LsmTreeRangeTable.this.delete(hashKey, rangeKey);
			}
		};
	}

	@Override
	public void deleteRange(final H hashKey) {
		try (TableIterator<RangeTableRow<H, R, V>> range = range(hashKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey) {
		try (TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, fromRangeKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		try (TableIterator<RangeTableRow<H, R, V>> range = range(hashKey, fromRangeKey, toRangeKey)) {
			while (range.hasNext()) {
				range.remove();
			}
		}
	}

}
