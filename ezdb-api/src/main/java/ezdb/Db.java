package ezdb;

import java.util.Comparator;
import ezdb.serde.Serde;

/**
 * The Db interface is used to create and delete tables. This is the entry point
 * into EZDB.
 * 
 * @author criccomini
 */
public interface Db {
  /**
   * Get a simple key/value table with the specified name and serdes. If the
   * table does not exist, it should be created. If the table exists, it should
   * be returned. If the table exists but the serdes don't match the
   * pre-existing table, a runtime exception should occur when the table is used
   * to read or write data.
   * 
   * @param tableName
   *          The name of the table. The table should be formatted to be
   *          compatible with filenames on the host operating system.
   * @param hashKeySerde
   *          The hash key serializer.
   * @param valueSerde
   *          The value serializer.
   * @return A simple key/value table.
   */
  public <H, V> Table<H, V> getTable(String tableName, Serde<H> hashKeySerde, Serde<V> valueSerde);

  /**
   * Get a hash/range table with the specified name and serdes. If the table
   * does not exist, it should be created. If the table exists, it should be
   * returned. If the table exists but the serdes don't match the pre-existing
   * table, a runtime exception should occur when the table is used to read or
   * write data.
   * 
   * @param tableName
   *          The name of the table. The table should be formatted to be
   *          compatible with filenames on the host operating system.
   * @param hashKeySerde
   *          The hash key serializer.
   * @param rangeKeySerde
   *          The range key serializer. If range keys are not used, any
   *          serializer can be supplied, as it will be ignored.
   * @param valueSerde
   *          The value serializer.
   * @return A hash/range table.
   */
  public <H, R, V> RangeTable<H, R, V> getTable(
      String tableName,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde);

  /**
   * Get a hash/range table with the specified name and serdes. If the table
   * does not exist, it should be created. If the table exists, it should be
   * returned. If the table exists but the serdes don't match the pre-existing
   * table, a runtime exception should occur when the table is used to read or
   * write data.
   * 
   * @param tableName
   *          The name of the table. The table should be formatted to be
   *          compatible with filenames on the host operating system.
   * @param hashKeySerde
   *          The hash key serializer.
   * @param rangeKeySerde
   *          The range key serializer. If range keys are not used, any
   *          serializer can be supplied, as it will be ignored.
   * @param valueSerde
   *          The value serializer.
   * @param hashKeyComparator
   *          A comparator to be used when sorting hash keys.
   * @param rangeKeyComparator
   *          A comparator to be used when sorting range keys that have the same
   *          hash key.
   * @return A hash/range table.
   */
  public <H, R, V> RangeTable<H, R, V> getTable(
      String tableName,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde,
      Comparator<byte[]> hashKeyComparator,
      Comparator<byte[]> rangeKeyComparator);

  /**
   * Delete a table from disk and memory.
   * 
   * @param tableName
   *          The logical name of the table to delete.
   */
  public void deleteTable(String tableName);
}
