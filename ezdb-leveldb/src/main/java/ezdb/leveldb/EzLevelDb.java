package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.iq80.leveldb.Options;

import ezdb.Db;
import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.ByteSerde;
import ezdb.serde.Serde;
import io.netty.buffer.ByteBuf;

/**
 * An implementation of Db that uses LevelDb tables to persist data. Each
 * "table" is just a LevelDB database persisted as a subdirectory inside of
 * EzLevelDb's root.
 * 
 * @author criccomini
 * 
 */
public class EzLevelDb implements Db<ByteBuf> {
	private final File root;
	private final Map<String, RangeTable<?, ?, ?>> cache;
	private final EzLevelDbFactory factory;

	public EzLevelDb(final File root) {
		this(root, new EzLevelDbJavaFactory());
	}

	public EzLevelDb(final File root, final EzLevelDbFactory factory) {
		this.root = root;
		this.factory = factory;
		this.cache = new HashMap<String, RangeTable<?, ?, ?>>();
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
		return getTable(tableName, hashKeySerde, ByteSerde.get, valueSerde);
	}

	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new LexicographicalComparator(),
				new LexicographicalComparator());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <H, R, V> RangeTable<H, R, V> getTable(final String tableName, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde, final Comparator<ByteBuf> hashKeyComparator,
			final Comparator<ByteBuf> rangeKeyComparator) {
		synchronized (cache) {
			RangeTable<?, ?, ?> table = cache.get(tableName);

			if (table == null) {
				table = new EzLevelDbTable<H, R, V>(new File(root, tableName), factory, hashKeySerde, rangeKeySerde,
						valueSerde, hashKeyComparator, rangeKeyComparator);
				cache.put(tableName, table);
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
