package ezdb.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.io.File;
import java.util.Random;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.IntegerSerde;
import ezdb.treemap.TreeMapTable;

/**
 * This is a little test that mostly just compares random behavior between a
 * mock implementation, and LevelDB. It's not really a valid performance test,
 * since it resets the tables frequently enough to never get a large data set,
 * and duplicates all actions in TreeMap.
 * 
 * @author criccomini
 * 
 */
public class TortureEzLevelDb {
  public static final float LOG_SECONDS = 10;

  public static void main(String[] args) throws InterruptedException {
    EzLevelDb ezdb = new EzLevelDb(new File("/tmp"));
    ezdb.deleteTable("torture-test");
    Random rand = new Random();
    RangeTable<Integer, Integer, Integer> table = ezdb.getTable("torture-test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
    RangeTable<Integer, Integer, Integer> mockTable = new TreeMapTable<Integer, Integer, Integer>(IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, LexicographicalComparator.get, LexicographicalComparator.get);
    long tables = 0, deletes = 0, writes = 0, reads = 0, rangeH = 0, rangeHF = 0, rangeHFT = 0, lastPrint = System.currentTimeMillis();

    while (true) {
      // pick something to do
      double pick = rand.nextDouble();
      int hashKey = rand.nextInt(500);
      int rangeKey = rand.nextInt(500);
      int value = rand.nextInt(500);

      if (pick < 0.00001d) {
        // .001% of the time, recycle the table
        table.close();
        ezdb.deleteTable("torture-test");
        table = ezdb.getTable("torture-test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
        mockTable = new TreeMapTable<Integer, Integer, Integer>(IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, LexicographicalComparator.get, LexicographicalComparator.get);
        ++tables;
      } else if (pick < 0.1) {
        // 10% of the time, delete
        table.delete(hashKey, rangeKey);
        mockTable.delete(hashKey, rangeKey);
        ++deletes;
      } else if (pick < 0.30) {
        // 20% of the time write
        table.put(hashKey, rangeKey, value);
        mockTable.put(hashKey, rangeKey, value);
        ++writes;
      } else if (pick < 0.60) {
        // 30% of the time read
        assertEquals(mockTable.get(hashKey, rangeKey), table.get(hashKey, rangeKey));
        ++reads;
      } else if (pick < 0.70) {
        // 10% of the time range(h)
        int hash = rand.nextInt(500);
        TableIterator<Integer, Integer, Integer> iterator = table.range(hash);
        TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash);

        while (iterator.hasNext() && mockIterator.hasNext()) {
          assertEquals(mockIterator.next(), iterator.next());
          ++rangeH;
        }

        assertFalse(iterator.hasNext());
        assertFalse(mockIterator.hasNext());
      } else if (pick < 0.80) {
        // 10% of the time range(h, f)
        int hash = rand.nextInt(500);
        int from = rand.nextInt(500);
        TableIterator<Integer, Integer, Integer> iterator = table.range(hash, from);
        TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash, from);

        while (iterator.hasNext() && mockIterator.hasNext()) {
          assertEquals(mockIterator.next(), iterator.next());
          ++rangeHF;
        }

        assertFalse(iterator.hasNext());
        assertFalse(mockIterator.hasNext());
      } else {
        // 20% of the time range(h, f, t)
        int hash = rand.nextInt(500);
        int from = rand.nextInt(500);
        int to = rand.nextInt(500);
        TableIterator<Integer, Integer, Integer> iterator = table.range(hash, from, to);
        TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash, from, to);

        while (iterator.hasNext() && mockIterator.hasNext()) {
          assertEquals(mockIterator.next(), iterator.next());
          ++rangeHFT;
        }

        assertFalse(iterator.hasNext());
        assertFalse(mockIterator.hasNext());
      }

      if (System.currentTimeMillis() - lastPrint > LOG_SECONDS * 1000) {
        lastPrint = System.currentTimeMillis();
        StringBuffer out = new StringBuffer().append("Table creations: ").append(tables / LOG_SECONDS).append("/s, deletes: ").append(deletes / LOG_SECONDS).append("/s, writes: ").append(writes / LOG_SECONDS).append("/s, reads: ").append(reads / LOG_SECONDS).append("/s, range(hash): ").append(rangeH / LOG_SECONDS).append("/s, range(hash, from): ").append(rangeHF / LOG_SECONDS).append("/s, range(hash, from, to): ").append(rangeHFT / LOG_SECONDS).append("/s, total reads: ").append((reads + rangeH + rangeHF + rangeHFT) / LOG_SECONDS).append("/s");

        tables = 0;
        deletes = 0;
        writes = 0;
        reads = 0;
        rangeH = 0;
        rangeHF = 0;
        rangeHFT = 0;

        System.out.println(out.toString());
      }
    }
  }
}
