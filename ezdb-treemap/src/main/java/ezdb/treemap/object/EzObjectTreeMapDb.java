package ezdb.treemap.object;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.ComparableComparator;
import ezdb.serde.ByteSerde;
import ezdb.serde.Serde;

public class EzObjectTreeMapDb implements Db<Object> {
	private final Map<String, RangeTable<?, ?, ?>> cache;

	public EzObjectTreeMapDb() {
		this.cache = new HashMap<String, RangeTable<?, ?, ?>>();
	}

	@Override
	public void deleteTable(final String tableName) {
		synchronized (cache) {
			cache.remove(tableName);
		}
	}

	@Override
	public <H, V> Table<H, V> getTable(final String tableName, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, ByteSerde.get, valueSerde);
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new ComparableComparator(),
				new ComparableComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<Object> hashKeyComparator,
			final Comparator<Object> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = cache.get(tableName);

			if (table == null) {
				table = newTable(hashKeyComparator, rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

	protected <R, H, V> ObjectTreeMapTable<H, R, V> newTable(final Comparator<H> hashKeyComparator,
			final Comparator<R> rangeKeyComparator) {
		return new ObjectTreeMapTable<H, R, V>(hashKeyComparator, rangeKeyComparator);
	}

}
