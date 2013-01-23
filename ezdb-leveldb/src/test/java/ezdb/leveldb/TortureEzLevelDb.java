package ezdb.leveldb;

import java.io.File;
import java.util.Random;

import ezdb.RangeTable;
import ezdb.serde.IntegerSerde;

public class TortureEzLevelDb {
  public static final int LOG_SECONDS = 10;
  
  public static void main(String[] args) throws InterruptedException {
    EzLevelDb ezdb = new EzLevelDb(new File("/tmp"));
    ezdb.deleteTable("torture-test");
    Random rand = new Random();
    RangeTable<Integer, Integer, Integer> table = ezdb.getTable("torture-test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
    long tables = 0, deletes = 0, writes = 0, reads = 0, lastPrint = System.currentTimeMillis();

    while (true) {
      // pick something to do
      double pick = rand.nextDouble();
      int hashKey = rand.nextInt(500);
      int rangeKey = rand.nextInt(500);
      int value = rand.nextInt(500);

      if (pick < 0.0001d) {
        // .01% of the time, recycle the table
        table.close();
        table = ezdb.getTable("torture-test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
        ++tables;
      } else if (pick < 0.1) {
        // 10% of the time, delete
        table.delete(hashKey, rangeKey);
        ++deletes;
      } else if (pick < 0.30) {
        // 20% of the time write
        table.put(hashKey, rangeKey, value);
        ++writes;
      } else {
        // 70% of the time read
        table.get(hashKey, rangeKey);
        ++reads;
      }

      if (System.currentTimeMillis() - lastPrint > LOG_SECONDS * 1000) {
        lastPrint = System.currentTimeMillis();
        StringBuffer out = new StringBuffer()
        .append("Table creations: ")
        .append(tables / LOG_SECONDS)
        .append("/s, deletes: ")
        .append(deletes / LOG_SECONDS)
        .append("/s, writes: ")
        .append(writes / LOG_SECONDS)
        .append("/s, reads: ")
        .append(reads / LOG_SECONDS)
        .append("/s");

        tables = 0;
        deletes = 0;
        writes = 0;
        reads = 0;

        System.out.println(out.toString());
      }
    }
  }
}
