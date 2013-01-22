package ezleveldb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import ezleveldb.serde.Serde;

public class Db {
  private final File root;
  private final Map<String, Table<?, ?, ?>> tables;

  public Db(File root) {
    this.root = root;
    this.tables = new HashMap<String, Table<?, ?, ?>>();
  }

  public void deleteTable(String tableName) throws IOException {
    tables.get(tableName).delete();
  }

  @SuppressWarnings("unchecked")
  public <P, O, V> Table<P, O, V> getTable(
      String tableName,
      Serde<P> partitionKeySerde,
      Serde<O> orderKeySerde,
      Serde<V> valueSerde) throws IOException {
    Table<P, O, V> table = (Table<P, O, V>) tables.get(tableName);

    if (table == null) {
      tables.put(tableName, new Table<P, O, V>(new File(root, tableName), partitionKeySerde, orderKeySerde, valueSerde));
      table = (Table<P, O, V>) tables.get(tableName);
    }

    return table;
  }
}
