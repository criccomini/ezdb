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

/**
 * An implementation of Db that uses LevelDb tables to persist data. Each
 * "table" is just a LevelDB database persisted as a subdirectory inside of
 * EzLevelDb's root.
 * 
 * @author criccomini
 * 
 */
public class EzObjectTreeMapDb implements Db<Object> {
	private final Map<String, RangeTable<?, ?, ?>> cache;

	public EzObjectTreeMapDb() {
		this.cache = new HashMap<String, RangeTable<?, ?, ?>>();
	}

	@Override
	public void deleteTable(String tableName) {
		synchronized (cache) {
			cache.remove(tableName);
		}
	}

	@Override
	public <H, V> Table<H, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, ByteSerde.get, valueSerde);
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new ComparableComparator(),
				new ComparableComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde, Comparator<Object> hashKeyComparator, Comparator<Object> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = cache.get(tableName);

			if (table == null) {
				table = new ObjectTreeMapTable<H, R, V>((Comparator<H>) hashKeyComparator,
						(Comparator<R>) rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

}
