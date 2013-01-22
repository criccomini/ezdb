package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.Options;

import ezdb.Db;
import ezdb.DbException;
import ezdb.Table;
import ezdb.serde.Serde;

public class EzLevelDb implements Db {
  private final File root;
  private final Map<String, Table<?, ?, ?>> tables;
  private final Object lock;

  public EzLevelDb(File root) {
    this.root = root;
    this.tables = new HashMap<String, Table<?, ?, ?>>();
    lock = new Object();
  }

  public void deleteTable(String tableName) {
    try {
      synchronized (lock) {
        tables.remove(tableName);
        JniDBFactory.factory.destroy(getFile(tableName), new Options());
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public <P, O, V> Table<P, O, V> getTable(String tableName, Serde<P> partitionKeySerde, Serde<O> orderKeySerde, Serde<V> valueSerde) {
    synchronized (lock) {
      Table<P, O, V> table = (Table<P, O, V>) tables.get(tableName);

      if (table == null) {
        try {
          tables.put(tableName, new EzLevelDbTable<P, O, V>(new File(root, tableName), partitionKeySerde, orderKeySerde, valueSerde));
        } catch (IOException e) {
          throw new DbException(e);
        }
        table = (Table<P, O, V>) tables.get(tableName);
      }

      return table;
    }
  }

  private File getFile(String tableName) {
    return new File(root, tableName);
  }
}
