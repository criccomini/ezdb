package ezleveldb;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
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
    this.path = path;
    this.db = JniDBFactory.factory.open(path, options);
    this.partitionKeySerde = partitionKeySerde;
    this.orderKeySerde = orderKeySerde;
    this.valueSerde = valueSerde;
  }

  public Iterator<V> get(P partitionKey, O orderKey) {
    final DBIterator iterator = db.iterator();
    byte[] keyBytes = combine(partitionKeySerde.toBytes(partitionKey), orderKeySerde.toBytes(orderKey));
    iterator.seek(keyBytes);
    return new Iterator<V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public V next() {
        return valueSerde.fromBytes(iterator.next().getValue());
      }

      @Override
      public void remove() {
        iterator.remove();
      }
    };
  }

  public Iterator<V> scan(P partitionKeyFrom, O orderKeyFrom, P partitionKeyTo, O orderKeyTo) {
    return null;
  }

  public void put(P partitionKey, O orderKey, V value) {
  }

  public static byte[] combine(byte[] arg1, byte[] arg2) {
    byte[] result = new byte[arg1.length + arg2.length];
    System.arraycopy(arg1, 0, result, 0, arg1.length);
    System.arraycopy(arg2, 0, result, arg1.length, arg2.length);
    return result;
  }

  public void delete() throws IOException {
    JniDBFactory.factory.destroy(path, new Options());
  }
}
