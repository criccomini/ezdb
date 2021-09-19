package ezdb.lsmtree;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.indeed.util.serialization.Serializer;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.ComparableComparator;
import ezdb.serde.ByteSerde;
import ezdb.serde.Serde;

public class EzLsmTreeDb implements Db<Object> {
	private final Map<String, RangeTable<?, ?, ?>> cache;

	public EzLsmTreeDb() {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<Object> hashKeyComparator,
			final Comparator<Object> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = cache.get(tableName);

			if (table == null) {
				table = newTable(EzdbSerializer.valueOf(hashKeySerde), EzdbSerializer.valueOf(rangeKeySerde),
						EzdbSerializer.valueOf(valueSerde), (Comparator) hashKeyComparator,
						(Comparator) rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

	public <R, H, V> LsmTreeTable<H, R, V> newTable(final Serializer<H> hashKeySerializer,
			final Serializer<R> rangeKeySerializer, final Serializer<V> valueSerializer,
			final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator) {
		return new LsmTreeTable<H, R, V>(hashKeySerializer, rangeKeySerializer, valueSerializer, hashKeyComparator,
				rangeKeyComparator);
	}

}
