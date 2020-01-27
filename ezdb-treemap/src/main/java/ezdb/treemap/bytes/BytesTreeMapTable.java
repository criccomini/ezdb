package ezdb.treemap.bytes;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

import ezdb.EmptyTableIterator;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.batch.Batch;
import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class BytesTreeMapTable<H, R, V> implements RangeTable<H, R, V> {
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final NavigableMap<byte[], byte[]> map;
	private final Comparator<byte[]> hashKeyComparator;
	private final Comparator<byte[]> rangeKeyComparator;

	public BytesTreeMapTable(Serde<H> hashKeySerde, Serde<R> rangeKeySerde, Serde<V> valueSerde,
			final Comparator<byte[]> hashKeyComparator, final Comparator<byte[]> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.map = new ConcurrentSkipListMap<byte[], byte[]>(new Comparator<byte[]>() {
			@Override
			public int compare(byte[] k1, byte[] k2) {
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
		map.put(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
	}

	@Override
	public V get(H hashKey, R rangeKey) {
		byte[] valueBytes = map.get(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
		if (valueBytes != null) {
			return valueSerde.fromBytes(valueBytes);
		}
		return null;
	}

	@Override
	public TableIterator<H, R, V> range(H hashKey) {
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
		final Iterator<Map.Entry<byte[], byte[]>> iterator = map.tailMap(keyBytesFrom, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				TableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final Iterator<Map.Entry<byte[], byte[]>> iterator = map.tailMap(keyBytesFrom, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) > 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<byte[], byte[]>> iterator = map.subMap(keyBytesFrom, true, keyBytesTo, true).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, next.getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		map.remove(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
	}

	@Override
	public void close() {
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(H hashKey) {
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
		final Iterator<Map.Entry<byte[], byte[]>> iterator = map.descendingMap().headMap(keyBytesFrom, true).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final Iterator<Map.Entry<byte[], byte[]>> iterator = map.descendingMap().tailMap(keyBytesFrom, true).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

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
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) < 0) {
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<byte[], byte[]>> iterator;
		if (fromRangeKey != null) {
			iterator = map.descendingMap().tailMap(keyBytesFrom, true).entrySet().iterator();
		} else {
			iterator = map.descendingMap().headMap(keyBytesFrom, true).entrySet().iterator();
		}
		return new TableIterator<H, R, V>() {
			Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

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
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = new RawTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
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
		}else {
			TableIterator<H, R, V> range = range(hashKey);
			if (range.hasNext()) {
				return range.next();
			}else {
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
				BytesTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(H hashKey) {
				BytesTreeMapTable.this.delete(hashKey);
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
				BytesTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(H hashKey) {
				BytesTreeMapTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(H hashKey, R rangeKey, V value) {
				BytesTreeMapTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(H hashKey, R rangeKey) {
				BytesTreeMapTable.this.delete(hashKey, rangeKey);
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
