package ezleveldb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import ezleveldb.serde.Serde;

public class Table<P, O, V> {
  private final DB db;
  private final Serde<P> partitionKeySerde;
  private final Serde<O> orderKeySerde;
  private final Serde<V> valueSerde;
  private final File path;

  public Table(File path, Serde<P> partitionKeySerde, Serde<O> orderKeySerde, Serde<V> valueSerde) throws IOException {
    Options options = new Options();
    options.createIfMissing(true);
    options.comparator(new HashRangeComparator());
    this.path = path;
    this.db = JniDBFactory.factory.open(path, options);
    this.partitionKeySerde = partitionKeySerde;
    this.orderKeySerde = orderKeySerde;
    this.valueSerde = valueSerde;
  }

  public Iterator<V> get(P partitionKey, O orderKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytes = combine(partitionKeySerde.toBytes(partitionKey), orderKeySerde.toBytes(orderKey));
    iterator.seek(keyBytes);
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext() && ByteBuffer.wrap(keyBytes).compareTo(ByteBuffer.wrap(iterator.peekNext().getKey())) < 1;
      }

      @Override
      public V next() {
        if (hasNext()) {
          return valueSerde.fromBytes(iterator.next().getValue());
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }

  public Iterator<V> scan(P partitionKeyFrom, O orderKeyFrom, P partitionKeyTo, O orderKeyTo) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = combine(partitionKeySerde.toBytes(partitionKeyFrom), orderKeySerde.toBytes(orderKeyFrom));
    final byte[] keyBytesTo = combine(partitionKeySerde.toBytes(partitionKeyFrom), orderKeySerde.toBytes(orderKeyFrom));
    iterator.seek(keyBytesFrom);
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext() && ByteBuffer.wrap(keyBytesTo).compareTo(ByteBuffer.wrap(iterator.peekNext().getKey())) < 1;
      }

      @Override
      public V next() {
        if (hasNext()) {
          return valueSerde.fromBytes(iterator.next().getValue());
        }
        throw new NoSuchElementException();
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }

  public void put(P partitionKey, V value) {
    put(partitionKey, null, value);
  }

  public void put(P partitionKey, O orderKey, V value) {
    db.put(combine(partitionKeySerde.toBytes(partitionKey), orderKeySerde.toBytes(orderKey)), valueSerde.toBytes(value));
  }

  public void append(P partitionKey, O orderKey, V value) {
    put(partitionKey, orderKey, value);
  }

  public void delete() throws IOException {
    JniDBFactory.factory.destroy(path, new Options());
  }

  public static byte[] combine(byte[] arg1, byte[] arg2) {
    byte[] result = new byte[8 + arg1.length + arg2.length];
    System.arraycopy(ByteBuffer.allocate(4).putInt(arg1.length).array(), 0, result, 0, 4);
    System.arraycopy(arg1, 0, result, 4, arg1.length);
    System.arraycopy(ByteBuffer.allocate(4).putInt(arg2.length).array(), 0, result, 4 + arg1.length, 4);
    System.arraycopy(arg2, 0, result, 8 + arg1.length, arg2.length);
    return result;
  }
}
