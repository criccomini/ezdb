package ezleveldb;

import java.util.HashMap;
import java.util.Map;
import ezleveldb.serde.Serde;

public class Db {
  private final String root;
  private final Map<String, Table> tables;

  public Db(String root) {
    this.root = root;
    this.tables = new HashMap<String, Table>();
  }

  public void deleteTable(String tableName) {
  }

  public <P, O, V> Table<P, O, V> getTable(
      String table,
      Serde<P> partitionKeySerde,
      Serde<O> orderKeySerde,
      Serde<V> valueSerde) {
    return null;
  }
}
