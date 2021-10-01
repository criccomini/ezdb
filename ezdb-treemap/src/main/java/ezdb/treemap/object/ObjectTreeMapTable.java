package ezdb.treemap.object;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import ezdb.EmptyTableIterator;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.util.ObjectTableKey;
import ezdb.util.ObjectTableRow;
import ezdb.util.Util;

public class ObjectTreeMapTable<H, R, V> implements RangeTable<H, R, V> {
	private final NavigableMap<ObjectTableKey<H, R>, V> map;
	private final Comparator<H> hashKeyComparator;
	private final Comparator<R> rangeKeyComparator;
	private final EzObjectTreeMapDbComparator<H, R> keyComparator;

	public ObjectTreeMapTable(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.keyComparator = new EzObjectTreeMapDbComparator<H, R>(hashKeyComparator, rangeKeyComparator);
		this.map = newMap(keyComparator);
	}

	protected NavigableMap<ObjectTableKey<H, R>, V> newMap(final Comparator<ObjectTableKey<H, R>> comparator) {
		return new ConcurrentSkipListMap<ObjectTableKey<H, R>, V>(comparator);
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
		final ObjectTableKey<H, R> key = Util.combine(hashKey, rangeKey, keyComparator);
		map.put(key, value);
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final V value = map.get(Util.combine(hashKey, rangeKey, keyComparator));
		return value;
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Set<Entry<ObjectTableKey<H, R>, V>> tailMapEntries = map.tailMap(keyBytesFrom, true).entrySet();
		final Iterator<Entry<ObjectTableKey<H, R>, V>> iterator = tailMapEntries.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				TableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
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
		final Set<Entry<ObjectTableKey<H, R>, V>> tailMapEntries = map.tailMap(keyBytesFrom, true).entrySet();
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = tailMapEntries.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
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
		final Set<Entry<ObjectTableKey<H, R>, V>> subMapEntries = map.subMap(keyBytesFrom, true, keyBytesTo, true)
				.entrySet();
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = subMapEntries.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, next.getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				ObjectTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
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
		map.remove(Util.combine(hashKey, rangeKey, keyComparator));
	}

	@Override
	public void close() {
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null, keyComparator);
		final Set<Entry<ObjectTableKey<H, R>, V>> headMapEntries = map.descendingMap().headMap(keyBytesFrom, true)
				.entrySet();
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = headMapEntries.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
				next();
			}

			@Override
			public void close() {
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey, keyComparator);
		final Set<Entry<ObjectTableKey<H, R>, V>> tailMapEntries = map.descendingMap().tailMap(keyBytesFrom, true)
				.entrySet();
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = tailMapEntries.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
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
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator;
		if (fromRangeKey != null) {
			final Set<Entry<ObjectTableKey<H, R>, V>> tailMapEntries = map.descendingMap().tailMap(keyBytesFrom, true)
					.entrySet();
			iterator = tailMapEntries.iterator();
		} else {
			final Set<Entry<ObjectTableKey<H, R>, V>> headMapEntries = map.descendingMap().headMap(keyBytesFrom, true)
					.entrySet();
			iterator = headMapEntries.iterator();
		}
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = new ObjectTableRow<H, R, V>(next);
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
				map.remove(next.getKey());
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
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, rangeKey, keyComparator);
		Entry<ObjectTableKey<H, R>, V> value = map.floorEntry(keyBytesFrom);
		if (value == null || Util.compareKeys(hashKeyComparator, null, keyBytesFrom, value.getKey()) != 0) {
			value = map.ceilingEntry(keyBytesFrom);
		}
		if (value == null || Util.compareKeys(hashKeyComparator, null, keyBytesFrom, value.getKey()) != 0) {
			return null;
		} else {
			return new ObjectTableRow<H, R, V>(value.getKey(), value.getValue());
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
				ObjectTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				ObjectTreeMapTable.this.delete(hashKey);
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
				ObjectTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				ObjectTreeMapTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(final H hashKey, final R rangeKey, final V value) {
				ObjectTreeMapTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(final H hashKey, final R rangeKey) {
				ObjectTreeMapTable.this.delete(hashKey, rangeKey);
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
