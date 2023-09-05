package ezdb.treemap.object;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import ezdb.Db;
import ezdb.comparator.ComparableComparator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.Serde;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;
import ezdb.treemap.object.table.ObjectTreeMapTable;
import ezdb.treemap.object.table.range.ObjectTreeMapRangeTable;

public class EzObjectTreeMapDb implements Db<Object> {
	private final Map<String, Table<?, ?>> cache;

	public EzObjectTreeMapDb() {
		this.cache = new HashMap<String, Table<?, ?>>();
	}

	@Override
	public void deleteTable(final String tableName) {
		synchronized (cache) {
			cache.remove(tableName);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, V> Table<H, V> getTable(final String tableName, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, valueSerde, new ComparableComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, V> Table<H, V> getTable(final String tableName, final Serde<H> hashKeySerde, final Serde<V> valueSerde,
			final Comparator<Object> hashKeyComparator) {
		synchronized (cache) {
			Table<?, ?> table = cache.get(tableName);

			if (table == null) {
				table = newTable(hashKeyComparator);
				cache.put(tableName, table);
			}

			return (Table<H, V>) table;
		}
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		return getRangeTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new ComparableComparator(),
				new ComparableComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<Object> hashKeyComparator,
			final Comparator<Object> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = (RangeTable<?, ?, ?>) cache.get(tableName);

			if (table == null) {
				table = newRangeTable(hashKeyComparator, rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

	protected <H, R, V> ObjectTreeMapRangeTable<H, R, V> newRangeTable(final Comparator<H> hashKeyComparator,
			final Comparator<R> rangeKeyComparator) {
		return new ObjectTreeMapRangeTable<H, R, V>(hashKeyComparator, rangeKeyComparator);
	}

	protected <H, V> ObjectTreeMapTable<H, V> newTable(final Comparator<H> hashKeyComparator) {
		return new ObjectTreeMapTable<H, V>(hashKeyComparator);
	}

}
