package ezdb;

import java.util.Comparator;
import ezdb.serde.Serde;

public interface Db {
  public <P, O, V> Table<P, O, V> getTable(
      String tableName,
      Serde<P> partitionKeySerde,
      Serde<O> orderKeySerde,
      Serde<V> valueSerde);

  public <P, O, V> Table<P, O, V> getTable(
      String tableName,
      Serde<P> partitionKeySerde,
      Serde<O> orderKeySerde,
      Serde<V> valueSerde,
      Comparator<byte[]> rangeComparator);

  public void deleteTable(String tableName);
}
