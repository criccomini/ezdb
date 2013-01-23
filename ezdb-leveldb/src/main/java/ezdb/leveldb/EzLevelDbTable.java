package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLevelDbTable<H, R, V> implements RangeTable<H, R, V> {
  private final DB db;
  private final Serde<H> hashKeySerde;
  private final Serde<R> rangeKeySerde;
  private final Serde<V> valueSerde;
  private final Comparator<byte[]> hashKeyComparator;
  private final Comparator<byte[]> rangeKeyComparator;

  public EzLevelDbTable(
      File path,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde,
      Comparator<byte[]> hashKeyComparator,
      Comparator<byte[]> rangeKeyComparator) {
    this.hashKeySerde = hashKeySerde;
    this.rangeKeySerde = rangeKeySerde;
    this.valueSerde = valueSerde;
    this.hashKeyComparator = hashKeyComparator;
    this.rangeKeyComparator = rangeKeyComparator;

    Options options = new Options();
    options.createIfMissing(true);
    options.comparator(new EzLevelDbComparator(hashKeyComparator, rangeKeyComparator));

    try {
      this.db = JniDBFactory.factory.open(path, options);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  public void put(H hashKey, V value) {
    put(hashKey, null, value);
  }

  @Override
  public void put(H hashKey, R rangeKey, V value) {
    db.put(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
  }

  @Override
  public V get(H hashKey) {
    return get(hashKey, null);
  }

  @Override
  public V get(H hashKey, R rangeKey) {
    byte[] valueBytes = db.get(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));

    if (valueBytes == null) {
      return null;
    }

    return valueSerde.fromBytes(valueBytes);
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
    iterator.seek(keyBytesFrom);
    return new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        return new EzLevelDbTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    iterator.seek(keyBytesFrom);
    return new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext() && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        return new EzLevelDbTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
    iterator.seek(keyBytesFrom);
    return new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext() && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, iterator.peekNext().getKey()) > 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        return new EzLevelDbTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  @Override
  public void delete(H hashKey) {
    delete(hashKey, null);
  }

  @Override
  public void delete(H hashKey, R rangeKey) {
    this.db.delete(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
  }

  @Override
  public void close() {
    this.db.close();
  }
}
