package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.Options;
import ezdb.Db;
import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.LexicographicalComparator;
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
public class EzLevelDb implements Db {
  private final File root;
  private final AtomicReference<Map<String, RangeTable<?, ?, ?>>> cache;
  private final Object createLevelDbLock = new Object();

  public EzLevelDb(File root) {
    this.root = root;
    this.cache = new AtomicReference<Map<String, RangeTable<?, ?, ?>>>(new HashMap<String, RangeTable<?, ?, ?>>());
  }

  @Override
  public void deleteTable(String tableName) {
    try {
      Map<String, RangeTable<?, ?, ?>> oldCache;
      Map<String, RangeTable<?, ?, ?>> newCache;
      do {
        oldCache = cache.get();
        newCache = new HashMap<String, RangeTable<?, ?, ?>>(oldCache);
        newCache.remove(tableName);
      } while (!cache.compareAndSet(oldCache, newCache));
      JniDBFactory.factory.destroy(getFile(tableName), new Options());
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  public <H, V> Table<H, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<V> valueSerde) {
    return getTable(tableName, hashKeySerde, ByteSerde.get, valueSerde);
  }

  @Override
  public <H, R, V> RangeTable<H, R, V> getTable(
      String tableName,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde) {
    return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new LexicographicalComparator(), new LexicographicalComparator());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <H, R, V> RangeTable<H, R, V> getTable(
      String tableName,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde,
      Comparator<byte[]> hashKeyComparator,
      Comparator<byte[]> rangeKeyComparator) {
    Map<String, RangeTable<?, ?, ?>> oldCache = cache.get();
    RangeTable<?, ?, ?> table = oldCache.get(tableName);

    while (table == null) {
      // We must lock when creating a LevelDB table since there's a race
      // condition at the file system level over who gets to create the manifest
      // and lock files.
      synchronized (createLevelDbLock) {
        table = new EzLevelDbTable<H, R, V>(new File(root, tableName), hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator, rangeKeyComparator);
      }

      Map<String, RangeTable<?, ?, ?>> newCache = new HashMap<String, RangeTable<?, ?, ?>>(oldCache);
      newCache.put(tableName, table);

      if (!cache.compareAndSet(oldCache, newCache)) {
        table.close();
        table = cache.get().get(tableName);
      }
    }

    return (RangeTable<H, R, V>) table;
  }

  /**
   * A helper method used to convert a table name to the location on disk where
   * this LevelDB database will be persisted.
   * 
   * @param tableName
   *          The logical name of the table.
   * @return The physical location of the directory where this table should be
   *         persisted.
   */
  private File getFile(String tableName) {
    return new File(root, tableName);
  }
}
