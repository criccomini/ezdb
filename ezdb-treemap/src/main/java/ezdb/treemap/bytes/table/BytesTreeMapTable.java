package ezdb.treemap.bytes.table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.RawTableRow;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.TableIterator;
import ezdb.util.Util;

public class BytesTreeMapTable<H, V> implements Table<H, V> {

	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final Map<ByteBuffer, ByteBuffer> map;
	private final Comparator<ByteBuffer> hashKeyComparator;

	public BytesTreeMapTable(final Serde<H> hashKeySerde, final Serde<V> valueSerde,
			final Comparator<ByteBuffer> hashKeyComparator) {
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
		this.hashKeyComparator = hashKeyComparator;
		final Comparator<ByteBuffer> comparator = new Comparator<ByteBuffer>() {
			@Override
			public int compare(final ByteBuffer k1, final ByteBuffer k2) {
				return Util.compareKeys(hashKeyComparator, k1, k2);
			}
		};
		this.map = newMap(comparator);
	}

	protected Map<ByteBuffer, ByteBuffer> newMap(final Comparator<ByteBuffer> comparator) {
//		return new ConcurrentSkipListMap<ByteBuffer, ByteBuffer>(comparator);
		// we can also use the faster alternative here
		return new ConcurrentHashMap<ByteBuffer, ByteBuffer>();
	}

	@Override
	public void put(final H hashKey, final V value) {
		map.put(ByteBuffer.wrap(hashKeySerde.toBytes(hashKey)), ByteBuffer.wrap(valueSerde.toBytes(value)));
	}

	@Override
	public V get(final H hashKey) {
		final ByteBuffer valueBytes = map.get(ByteBuffer.wrap(hashKeySerde.toBytes(hashKey)));
		if (valueBytes != null) {
			valueBytes.clear();
			return valueSerde.fromBuffer(valueBytes);
		}
		return null;
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final Iterator<Map.Entry<ByteBuffer, ByteBuffer>> iterator = map.entrySet().iterator();
		return new TableIterator<TableRow<H, V>>() {
			Map.Entry<ByteBuffer, ByteBuffer> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public TableRow<H, V> next() {
				TableRow<H, V> row = null;

				if (hasNext()) {
					row = RawTableRow.valueOfBuffer(next, hashKeySerde, valueSerde);
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
		map.remove(ByteBuffer.wrap(hashKeySerde.toBytes(hashKey)));
	}

	@Override
	public void close() {
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

}
