package ezdb.treemap.bytes;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import ezdb.Db;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.Serde;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;
import ezdb.treemap.bytes.table.BytesTreeMapTable;
import ezdb.treemap.bytes.table.range.BytesTreeMapRangeTable;

public class EzBytesTreeMapDb implements Db<ByteBuffer> {
	private final Map<String, Table<?, ?>> cache;

	public EzBytesTreeMapDb() {
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
		return getTable(tableName, hashKeySerde, valueSerde, new LexicographicalComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, V> Table<H, V> getTable(final String tableName, final Serde<H> hashKeySerde, final Serde<V> valueSerde,
			final Comparator<ByteBuffer> hashKeyComparator) {
		synchronized (cache) {
			Table<?, ?> table = cache.get(tableName);

			if (table == null) {
				table = newTable(hashKeySerde, valueSerde, hashKeyComparator);
				cache.put(tableName, table);
			}

			return (Table<H, V>) table;
		}
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		return getRangeTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new LexicographicalComparator(),
				new LexicographicalComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, R, V> RangeTable<H, R, V> getRangeTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = (RangeTable<?, ?, ?>) cache.get(tableName);

			if (table == null) {
				table = newRangeTable(hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator, rangeKeyComparator);
				cache.put(tableName, table);
			}

			return (RangeTable<H, R, V>) table;
		}
	}

	protected <H, R, V> BytesTreeMapRangeTable<H, R, V> newRangeTable(final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuffer> hashKeyComparator,
			final Comparator<ByteBuffer> rangeKeyComparator) {
		return new BytesTreeMapRangeTable<H, R, V>(hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator,
				rangeKeyComparator);
	}

	protected <H, V> BytesTreeMapTable<H, V> newTable(final Serde<H> hashKeySerde, final Serde<V> valueSerde,
			final Comparator<ByteBuffer> hashKeyComparator) {
		return new BytesTreeMapTable<H, V>(hashKeySerde, valueSerde, hashKeyComparator);
	}

}
