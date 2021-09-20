package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.ComparableComparator;
import ezdb.serde.Serde;
import ezdb.serde.VoidSerde;

public class EzLsmTreeDb implements Db<Object> {
	private final Map<String, RangeTable<?, ?, ?>> cache;
	private final File root;
	private final EzLsmTreeDbFactory factory;

	public EzLsmTreeDb(final File root, final EzLsmTreeDbFactory factory) {
		this.cache = new HashMap<String, RangeTable<?, ?, ?>>();
		this.root = root;
		this.factory = factory;
	}

	@Override
	public void deleteTable(final String tableName) {
		synchronized (cache) {
			final RangeTable<?, ?, ?> removed = cache.remove(tableName);
			if (removed != null) {
				removed.close();
				try {
					factory.destroy(getFile(tableName));
				} catch (final IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	@Override
	public <H, V> Table<H, V> getTable(final String tableName, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, VoidSerde.get, valueSerde);
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new ComparableComparator(),
				new ComparableComparator());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<Object> hashKeyComparator,
			final Comparator<Object> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = cache.get(tableName);

			if (table == null) {
				table = newTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, (Comparator) hashKeyComparator,
						(Comparator) rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

	public <R, H, V> LsmTreeTable<H, R, V> newTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<H> hashKeyComparator,
			final Comparator<R> rangeKeyComparator) {
		return new LsmTreeTable<H, R, V>(new File(root, tableName), factory, hashKeySerde, rangeKeySerde, valueSerde,
				hashKeyComparator, rangeKeyComparator);
	}

	/**
	 * A helper method used to convert a table name to the location on disk where
	 * this LevelDB database will be persisted.
	 * 
	 * @param tableName The logical name of the table.
	 * @return The physical location of the directory where this table should be
	 *         persisted.
	 */
	private File getFile(final String tableName) {
		return new File(root, tableName);
	}

}
