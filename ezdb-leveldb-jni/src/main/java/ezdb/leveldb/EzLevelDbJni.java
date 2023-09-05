package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.iq80.leveldb.Options;

import ezdb.Db;
import ezdb.DbException;
import ezdb.comparator.LexicographicalComparator;
import ezdb.leveldb.table.EzLevelDbJniTable;
import ezdb.leveldb.table.range.EzLevelDbJniRangeTable;
import ezdb.serde.Serde;
import ezdb.table.Table;
import ezdb.table.range.RangeTable;

/**
 * An implementation of Db that uses LevelDb tables to persist data. Each
 * "table" is just a LevelDB database persisted as a subdirectory inside of
 * EzLevelDb's root.
 * 
 * @author criccomini
 * 
 */
public class EzLevelDbJni implements Db<ByteBuffer> {
	private final File root;
	private final Map<String, Table<?, ?>> cache;
	private final EzLevelDbJniFactory factory;

	public EzLevelDbJni(final File root) {
		this(root, new EzLevelDbJniFactory());
	}

	public EzLevelDbJni(final File root, final EzLevelDbJniFactory factory) {
		this.root = root;
		this.factory = factory;
		this.cache = new HashMap<String, Table<?, ?>>();
	}

	@Override
	public void deleteTable(final String tableName) {
		try {
			synchronized (cache) {
				cache.remove(tableName);
				factory.destroy(getFile(tableName), new Options());
			}
		} catch (final IOException e) {
			throw new DbException(e);
		}
	}

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
				table = new EzLevelDbJniTable<H, V>(new File(root, tableName), factory, hashKeySerde, valueSerde,
						hashKeyComparator);
				cache.put(tableName, table);
			} else if (!(table instanceof EzLevelDbJniTable)) {
				throw new IllegalStateException("Expected " + EzLevelDbJniTable.class.getSimpleName() + " but got "
						+ table.getClass().getSimpleName() + " for: " + tableName);
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
				table = new EzLevelDbJniRangeTable<H, R, V>(new File(root, tableName), factory, hashKeySerde,
						rangeKeySerde, valueSerde, hashKeyComparator, rangeKeyComparator);
				cache.put(tableName, table);
			} else if (!(table instanceof EzLevelDbJniRangeTable)) {
				throw new IllegalStateException("Expected " + EzLevelDbJniRangeTable.class.getSimpleName() + " but got "
						+ table.getClass().getSimpleName() + " for: " + tableName);
			}

			return (RangeTable<H, R, V>) table;
		}
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
