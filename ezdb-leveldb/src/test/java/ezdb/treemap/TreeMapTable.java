package ezdb.treemap;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.leveldb.EzLevelDbTableRow;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class TreeMapTable<H, R, V> implements RangeTable<H, R, V> {
  private final Serde<H> hashKeySerde;
  private final Serde<R> rangeKeySerde;
  private final Serde<V> valueSerde;
  private final NavigableMap<byte[], byte[]> map;
  private final Comparator<byte[]> hashKeyComparator;

  public TreeMapTable(
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde,
      final Comparator<byte[]> hashKeyComparator,
      final Comparator<byte[]> rangeKeyComparator) {
    this.hashKeySerde = hashKeySerde;
    this.rangeKeySerde = rangeKeySerde;
    this.valueSerde = valueSerde;
    this.hashKeyComparator = hashKeyComparator;
    this.map = new TreeMap<byte[], byte[]>(new Comparator<byte[]>() {
      @Override
      public int compare(byte[] k1, byte[] k2) {
        return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
      }
    });
  }

  @Override
  public void put(H hashKey, V value) {
    put(hashKey, null, value);
  }

  @Override
  public V get(H hashKey) {
    return get(hashKey, null);
  }

  @Override
  public void put(H hashKey, R rangeKey, V value) {
    map.put(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
  }

  @Override
  public V get(H hashKey, R rangeKey) {
    byte[] valueBytes = map.get(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
    if (valueBytes != null) {
      return valueSerde.fromBytes(valueBytes);
    }
    return null;
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey) {
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
    final Iterator<Map.Entry<byte[], byte[]>> iterator = map.tailMap(keyBytesFrom).entrySet().iterator();
    return new TableIterator<H, R, V>() {
      Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

      @Override
      public boolean hasNext() {
        return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        TableRow<H, R, V> row = null;

        if (hasNext()) {
          row = new EzLevelDbTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
        }

        if (iterator.hasNext()) {
          next = iterator.next();
        } else {
          next = null;
        }

        if (row != null) {
          return row;
        } else {
          throw new DbException(new NoSuchMethodException());
        }
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey) {
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    final Iterator<Map.Entry<byte[], byte[]>> iterator = map.tailMap(keyBytesFrom).entrySet().iterator();
    return new TableIterator<H, R, V>() {
      Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

      @Override
      public boolean hasNext() {
        return next != null && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, next.getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        EzLevelDbTableRow<H, R, V> row = null;

        if (hasNext()) {
          row = new EzLevelDbTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
        }

        if (iterator.hasNext()) {
          next = iterator.next();
        } else {
          next = null;
        }

        if (row != null) {
          return row;
        } else {
          throw new DbException(new NoSuchMethodException());
        }
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey) {
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
    final Iterator<Map.Entry<byte[], byte[]>> iterator = map.subMap(keyBytesFrom, true, keyBytesTo, false).entrySet().iterator();
    return new TableIterator<H, R, V>() {
      Map.Entry<byte[], byte[]> next = (iterator.hasNext()) ? iterator.next() : null;

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public TableRow<H, R, V> next() {
        EzLevelDbTableRow<H, R, V> row = null;

        if (hasNext()) {
          row = new EzLevelDbTableRow<H, R, V>(next, hashKeySerde, rangeKeySerde, valueSerde);
        }

        if (iterator.hasNext()) {
          next = iterator.next();
        } else {
          next = null;
        }

        if (row != null) {
          return row;
        } else {
          throw new DbException(new NoSuchMethodException());
        }
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
      }
    };
  }

  @Override
  public void delete(H hashKey) {
    delete(hashKey, null);
  }

  @Override
  public void delete(H hashKey, R rangeKey) {
    map.remove(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
  }

  @Override
  public void close() {
  }
}
