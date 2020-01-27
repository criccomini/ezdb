package ezdb.treemap.object;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
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

	public ObjectTreeMapTable(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.map = new ConcurrentSkipListMap<ObjectTableKey<H, R>, V>(new Comparator<ObjectTableKey<H, R>>() {
			@Override
			public int compare(ObjectTableKey<H, R> k1, ObjectTableKey<H, R> k2) {
				if(k1.toString().startsWith("ObjectTableKey [hashKey=JFOREX_EURUSD_Ask_Renko[_Time[1 HOUR]_atr(20)]")) {
					System.out.println(k1+ " "+k1);
				}
				return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
			}
		});
	}

	@Override
	public void put(H hashKey, V value) {
		put(hashKey, null, value);
	}

	@Override
	public V get(H hashKey) {
		return get(hashKey, null);
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		map.put(Util.combine(hashKey, rangeKey), value);
	}

	@Override
	public V get(H hashKey, R rangeKey) {
		V value = map.get(Util.combine(hashKey, rangeKey));
		return value;
	}

	@Override
	public TableIterator<H, R, V> range(H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null);
		final Iterator<Entry<ObjectTableKey<H, R>, V>> iterator = map.tailMap(keyBytesFrom).entrySet().iterator();
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
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final ObjectTableKey<H, R>  keyBytesFrom = Util.combine(hashKey, fromRangeKey);
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = map.tailMap(keyBytesFrom).entrySet().iterator();
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
	public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey);
		final ObjectTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) > 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = map.subMap(keyBytesFrom, true, keyBytesTo, true).entrySet()
				.iterator();
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
	public void delete(H hashKey) {
		delete(hashKey, null);
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		map.remove(Util.combine(hashKey, rangeKey));
	}

	@Override
	public void close() {
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(H hashKey) {
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, null);
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = map.descendingMap().headMap(keyBytesFrom).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
					System.out.println(next);
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
	public TableIterator<H, R, V> rangeReverse(H hashKey, R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey);
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator = map.descendingMap().tailMap(keyBytesFrom).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ObjectTableKey<H, R>, V> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) != 0
						&& iterator.hasNext()) {
					if(next.toString().startsWith("ObjectTableKey [hashKey=JFOREX_EURUSD_Ask_Renko[_Time[1 HOUR]_atr(20)]-2003_06_01_00_00_00_000-2003_06_30_23_59_59_999")) {
						System.out.println(next);
					}
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
	public TableIterator<H, R, V> rangeReverse(H hashKey, R fromRangeKey, R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final ObjectTableKey<H, R> keyBytesFrom = Util.combine(hashKey, fromRangeKey);
		final ObjectTableKey<H, R> keyBytesTo = Util.combine(hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) < 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ObjectTableKey<H, R>, V>> iterator;
		if (fromRangeKey != null) {
			iterator = map.descendingMap().tailMap(keyBytesFrom).entrySet().iterator();
		} else {
			iterator = map.descendingMap().headMap(keyBytesFrom).entrySet().iterator();
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
	public TableRow<H, R, V> getLatest(H hashKey) {
		TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey);
		if (rangeReverse.hasNext()) {
			return rangeReverse.next();
		} else {
			return null;
		}
	}

	@Override
	public TableRow<H, R, V> getLatest(H hashKey, R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
		if (rangeReverse.hasNext()) {
			return rangeReverse.next();
		} else {
			TableIterator<H, R, V> range = range(hashKey);
			if (range.hasNext()) {
				return range.next();
			} else {
				return null;
			}
		}
	}

	@Override
	public TableRow<H, R, V> getNext(H hashKey, R rangeKey) {
		TableIterator<H, R, V> range = range(hashKey, rangeKey);
		if (range.hasNext()) {
			return range.next();
		}
		return null;
	}

	@Override
	public TableRow<H, R, V> getPrev(H hashKey, R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		} else {
			TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
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
			public void put(H hashKey, V value) {
				ObjectTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(H hashKey) {
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
			public void put(H hashKey, V value) {
				ObjectTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(H hashKey) {
				ObjectTreeMapTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(H hashKey, R rangeKey, V value) {
				ObjectTreeMapTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(H hashKey, R rangeKey) {
				ObjectTreeMapTable.this.delete(hashKey, rangeKey);
			}
		};
	}

	@Override
	public void deleteRange(H hashKey) {
		TableIterator<H, R, V> range = range(hashKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

	@Override
	public void deleteRange(H hashKey, R fromRangeKey) {
		TableIterator<H, R, V> range = range(hashKey, fromRangeKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

	@Override
	public void deleteRange(H hashKey, R fromRangeKey, R toRangeKey) {
		TableIterator<H, R, V> range = range(hashKey, fromRangeKey, toRangeKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

}
