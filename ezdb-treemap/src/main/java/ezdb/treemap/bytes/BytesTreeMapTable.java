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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class BytesTreeMapTable<H, R, V> implements RangeTable<H, R, V> {

	private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final NavigableMap<ByteBuf, ByteBuf> map;
	private final Comparator<ByteBuf> hashKeyComparator;
	private final Comparator<ByteBuf> rangeKeyComparator;

	public BytesTreeMapTable(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde,
			final Comparator<ByteBuf> hashKeyComparator, final Comparator<ByteBuf> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		this.map = new ConcurrentSkipListMap<ByteBuf, ByteBuf>(new Comparator<ByteBuf>() {
			@Override
			public int compare(final ByteBuf k1, final ByteBuf k2) {
				return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
			}
		});
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
		map.put(Unpooled.wrappedBuffer(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)),
				Unpooled.wrappedBuffer(valueSerde.toBytes(value)));
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final ByteBuf keyBuffer = allocator.heapBuffer();
		try {
			Util.combine(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			final ByteBuf valueBytes = map.get(keyBuffer);
			if (valueBytes != null) {
				valueBytes.resetReaderIndex();
				return valueSerde.fromBuffer(valueBytes);
			}
			return null;
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
		}
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator = map.tailMap(keyBytesFrom, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				TableRow<H, R, V> row = null;

				if (hasNext()) {
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return range(hashKey);
		}
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator = map.tailMap(keyBytesFrom, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
			}
		};
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return range(hashKey, fromRangeKey);
		}
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuf keyBytesTo = allocator.heapBuffer();
		Util.combine(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) > 0) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			keyBytesTo.release(keyBytesTo.refCnt());
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator = map.subMap(keyBytesFrom, true, keyBytesTo, true)
				.entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, next.getKey()) >= 0;
			}

			@Override
			public TableRow<H, R, V> next() {
				RawTableRow<H, R, V> row = null;

				if (hasNext()) {
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
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
		final ByteBuf buffer = allocator.heapBuffer();
		Util.combine(buffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		try {
			map.remove(buffer);
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public void close() {
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator = map.descendingMap().headMap(keyBytesFrom, true)
				.entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
		if (fromRangeKey == null) {
			return rangeReverse(hashKey);
		}
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator = map.descendingMap().tailMap(keyBytesFrom, true)
				.entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				next = null;
			}
		};
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		if (toRangeKey == null) {
			return rangeReverse(hashKey, fromRangeKey);
		}
		final ByteBuf keyBytesFrom = allocator.heapBuffer();
		Util.combine(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuf keyBytesTo = allocator.heapBuffer();
		Util.combine(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFrom, keyBytesTo) < 0) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			keyBytesTo.release(keyBytesTo.refCnt());
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ByteBuf, ByteBuf>> iterator;
		if (fromRangeKey != null) {
			iterator = map.descendingMap().tailMap(keyBytesFrom, true).entrySet().iterator();
		} else {
			iterator = map.descendingMap().headMap(keyBytesFrom, true).entrySet().iterator();
		}
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuf, ByteBuf> next = (iterator.hasNext()) ? iterator.next() : null;

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
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, rangeKeySerde, valueSerde);
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
				keyBytesFrom.release(keyBytesFrom.refCnt());
				keyBytesTo.release(keyBytesTo.refCnt());
				next = null;
			}
		};
	}

	@Override
	public TableRow<H, R, V> getLatest(final H hashKey) {
		final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey);
		if (rangeReverse.hasNext()) {
			return rangeReverse.next();
		} else {
			return null;
		}
	}

	@Override
	public TableRow<H, R, V> getLatest(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		}
		final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey);
		if (rangeReverse.hasNext()) {
			return rangeReverse.next();
		} else {
			final TableIterator<H, R, V> range = range(hashKey);
			if (range.hasNext()) {
				return range.next();
			} else {
				return null;
			}
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
				BytesTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
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
			public void put(final H hashKey, final V value) {
				BytesTreeMapTable.this.put(hashKey, value);
			}

			@Override
			public void delete(final H hashKey) {
				BytesTreeMapTable.this.delete(hashKey);
			}

			@Override
			public void flush() {
			}

			@Override
			public void put(final H hashKey, final R rangeKey, final V value) {
				BytesTreeMapTable.this.put(hashKey, rangeKey, value);
			}

			@Override
			public void delete(final H hashKey, final R rangeKey) {
				BytesTreeMapTable.this.delete(hashKey, rangeKey);
			}
		};
	}

	@Override
	public void deleteRange(final H hashKey) {
		final TableIterator<H, R, V> range = range(hashKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, fromRangeKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

	@Override
	public void deleteRange(final H hashKey, final R fromRangeKey, final R toRangeKey) {
		final TableIterator<H, R, V> range = range(hashKey, fromRangeKey, toRangeKey);
		while (range.hasNext()) {
			range.remove();
		}
	}

}
