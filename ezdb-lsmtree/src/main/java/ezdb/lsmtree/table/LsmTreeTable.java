package ezdb.lsmtree.table;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.indeed.lsmtree.core.Store;

import ezdb.lsmtree.EzLsmTreeDbFactory;
import ezdb.lsmtree.EzdbSerializer;
import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.table.Table;
import ezdb.table.TableRow;
import ezdb.util.ObjectTableRow;
import ezdb.util.TableIterator;

public class LsmTreeTable<H, V> implements Table<H, V> {
	private final Store<H, V> store;
	private final Comparator<H> hashKeyComparator;
	private final EzLsmTreeDbComparator<H> keyComparator;

	public LsmTreeTable(final File path, final EzLsmTreeDbFactory factory, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde, final Comparator<H> hashKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.keyComparator = new EzLsmTreeDbComparator<H>(hashKeyComparator);
		try {
			this.store = factory.open(path, EzdbSerializer.valueOf(hashKeySerde), EzdbSerializer.valueOf(valueSerde),
					keyComparator);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(final H hashKey, final V value) {
		try {
			store.put(hashKey, value);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(final H hashKey) {
		try {
			final V value = store.get(hashKey);
			return value;
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TableIterator<TableRow<H, V>> range() {
		final Iterator<Store.Entry<H, V>> iterator;
		try {
			iterator = store.iterator();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
		return new TableIterator<TableRow<H, V>>() {
			private Store.Entry<H, V> next = (iterator.hasNext()) ? iterator.next() : null;

			@Override
			public boolean hasNext() {
				return next != null;
			}

			@Override
			public TableRow<H, V> next() {
				TableRow<H, V> row = null;

				if (hasNext()) {
					row = new ObjectTableRow<H, V>(next.getKey(), next.getValue());
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
		try {
			store.delete(hashKey);
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

}
