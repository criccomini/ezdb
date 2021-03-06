package ezdb.treemap.bytes;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.ByteSerde;
import ezdb.serde.Serde;

public class EzBytesTreeMapDb implements Db<byte[]> {
  private final Map<String, RangeTable<?, ?, ?>> cache;

  public EzBytesTreeMapDb() {
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
  public <H, R, V> RangeTable<H, R, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<R> rangeKeySerde, Serde<V> valueSerde) {
    return getTable(tableName, hashKeySerde, rangeKeySerde, valueSerde, new LexicographicalComparator(), new LexicographicalComparator());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <H, R, V> RangeTable<H, R, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<R> rangeKeySerde, Serde<V> valueSerde, Comparator<byte[]> hashKeyComparator, Comparator<byte[]> rangeKeyComparator) {
    synchronized (cache) {
      RangeTable<?, ?, ?> table = cache.get(tableName);

      if (table == null) {
        table = new BytesTreeMapTable<H, R, V>(hashKeySerde, rangeKeySerde, valueSerde, hashKeyComparator, rangeKeyComparator);
        cache.put(tableName, table);
      }

      return (RangeTable<H, R, V>) table;
    }
  }

}
