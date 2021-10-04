package ezdb.treemap.object.table;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.ObjectTableRow;
import ezdb.util.TableIterator;

public class ObjectTreeMapTable<H, V> implements Table<H, V> {
	private final Map<H, V> map;
	private final Comparator<H> hashKeyComparator;
	private final EzObjectTreeMapDbComparator<H> keyComparator;

	public ObjectTreeMapTable(final Comparator<H> hashKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.keyComparator = new EzObjectTreeMapDbComparator<H>(hashKeyComparator);
		this.map = newMap(keyComparator);
	}

	protected Map<H, V> newMap(final Comparator<H> comparator) {
//		return new ConcurrentSkipListMap<>(comparator);
		// we can also use the faster alternative here
		return new ConcurrentHashMap<H, V>();
	}

	@Override
	public void put(final H hashKey, final V value) {
		map.put(hashKey, value);
	}

	@Override
	public V get(final H hashKey) {
		final V value = map.get(hashKey);
		return value;
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final Iterator<Entry<H, V>> iterator = map.entrySet().iterator();
		return new TableIterator<TableRow<H, V>>() {
			Map.Entry<H, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public TableRow<H, V> next() {
				TableRow<H, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, V>(next);
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
		map.remove(hashKey);
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

}
