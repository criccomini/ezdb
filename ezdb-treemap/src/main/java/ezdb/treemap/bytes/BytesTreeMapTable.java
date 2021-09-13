package ezdb.treemap.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
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

public class BytesTreeMapTable<H, R, V> implements RangeTable<H, R, V> {

	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final NavigableMap<ByteBuffer, ByteBuffer> map;
	private final Comparator<ByteBuffer> hashKeyComparator;
	private final Comparator<ByteBuffer> rangeKeyComparator;

	public BytesTreeMapTable(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde,
			final Comparator<ByteBuffer> hashKeyComparator, final Comparator<ByteBuffer> rangeKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
		final Comparator<ByteBuffer> comparator = new Comparator<ByteBuffer>() {
			@Override
			public int compare(final ByteBuffer k1, final ByteBuffer k2) {
				return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
			}
		};
		this.map = newMap(comparator);
	}

	protected ConcurrentSkipListMap<ByteBuffer, ByteBuffer> newMap(final Comparator<ByteBuffer> comparator) {
		return new ConcurrentSkipListMap<ByteBuffer, ByteBuffer>(comparator);
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
		map.put(Util.combineBuffer(hashKeySerde, rangeKeySerde, hashKey, rangeKey),
				ByteBuffer.wrap(valueSerde.toBytes(value)));
	}

	@Override
	public V get(final H hashKey, final R rangeKey) {
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
		try {
			Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			final ByteBuffer valueBytes = map.get(keyBuffer.nioBuffer());
			if (valueBytes != null) {
				valueBytes.clear();
				return valueSerde.fromBuffer(valueBytes);
			}
			return null;
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
		}
	}

	@Override
	public TableIterator<H, R, V> range(final H hashKey) {
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.tailMap(keyBytesFromBuffer, true).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) == 0;
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
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.tailMap(keyBytesFromBuffer, true).entrySet()
				.iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) == 0;
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
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final ByteBuf keyBytesTo = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesToBuffer = keyBytesTo.nioBuffer();
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFromBuffer, keyBytesToBuffer) > 0) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			keyBytesTo.release(keyBytesTo.refCnt());
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map
				.subMap(keyBytesFromBuffer, true, keyBytesToBuffer, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesToBuffer,
						next.getKey()) >= 0;
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
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(buffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		try {
			map.remove(buffer.nioBuffer());
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public void close() {
	}

	@Override
	public TableIterator<H, R, V> rangeReverse(final H hashKey) {
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, null);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.descendingMap()
				.headMap(keyBytesFromBuffer, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) == 0;
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
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.descendingMap()
				.tailMap(keyBytesFromBuffer, true).entrySet().iterator();
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null
						&& Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) == 0;
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
		final ByteBuf keyBytesFrom = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesFrom, hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
		final ByteBuffer keyBytesFromBuffer = keyBytesFrom.nioBuffer();
		final ByteBuf keyBytesTo = ByteBufAllocator.DEFAULT.heapBuffer();
		Util.combineBuf(keyBytesTo, hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
		final ByteBuffer keyBytesToBuffer = keyBytesTo.nioBuffer();
		if (fromRangeKey != null
				&& Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesFromBuffer, keyBytesToBuffer) < 0) {
			keyBytesFrom.release(keyBytesFrom.refCnt());
			keyBytesTo.release(keyBytesTo.refCnt());
			return EmptyTableIterator.get();
		}
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator;
		if (fromRangeKey != null) {
			iterator = map.descendingMap().tailMap(keyBytesFromBuffer, true).entrySet().iterator();
		} else {
			iterator = map.descendingMap().headMap(keyBytesFromBuffer, true).entrySet().iterator();
		}
		return new TableIterator<H, R, V>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			{
				while (next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFromBuffer, next.getKey()) != 0
						&& iterator.hasNext()) {
					next = iterator.next();
				}
			}

			@Override
			public boolean hasNext() {
				return next != null && Util.compareKeys(hashKeyComparator, rangeKeyComparator, next.getKey(),
						keyBytesToBuffer) >= 0;
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
		try (TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey)) {
			if (rangeReverse.hasNext()) {
				return rangeReverse.next();
			} else {
				try (TableIterator<H, R, V> range = range(hashKey)) {
					if (range.hasNext()) {
						return range.next();
					} else {
						return null;
					}
				}
			}
		}
	}

	@Override
	public TableRow<H, R, V> getNext(final H hashKey, final R rangeKey) {
		try (TableIterator<H, R, V> range = range(hashKey, rangeKey)) {
			if (range.hasNext()) {
				return range.next();
			}
			return null;
		}
	}

	@Override
	public TableRow<H, R, V> getPrev(final H hashKey, final R rangeKey) {
		if (rangeKey == null) {
			return getLatest(hashKey);
		} else {
			try (final TableIterator<H, R, V> rangeReverse = rangeReverse(hashKey, rangeKey)) {
				if (rangeReverse.hasNext()) {
					return rangeReverse.next();
				}
				return null;
			}
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
