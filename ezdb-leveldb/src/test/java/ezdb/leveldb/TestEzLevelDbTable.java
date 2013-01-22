package ezdb.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ezdb.Db;
import ezdb.RangeTable;
import ezdb.Table;
import ezdb.TableIterator;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.IntegerSerde;
import ezdb.serde.StringSerde;
import ezdb.serde.VersionedSerde;
import ezdb.serde.VersionedSerde.Versioned;

public class TestEzLevelDbTable {
  private Db ezdb;
  private RangeTable<Integer, Integer, Integer> table;

  @Test
  public void testNulls() {
    // test nulls
    assertEquals(null, table.get(1));
    assertEquals(null, table.get(1, 1));
    assertTrue(!table.range(1).hasNext());
    assertTrue(!table.range(1, 2).hasNext());
    assertTrue(!table.range(1, 1, 2).hasNext());
    table.delete(1);
    table.delete(1, 1);
  }

  @Test
  public void testPutGetH() {
    Table<Integer, Integer> table = ezdb.getTable("test-simple", IntegerSerde.get, IntegerSerde.get);
    table.put(1, 1);
    assertEquals(new Integer(1), table.get(1));
    table.put(1, 2);
    assertEquals(new Integer(2), table.get(1));
    table.close();
  }

  @Test
  public void testPutGetHR() {
    table.put(1, 1);
    table.put(1, 1, 3);
    assertEquals(new Integer(1), table.get(1));
    assertEquals(new Integer(3), table.get(1, 1));
    table.put(1, 1, 4);
    assertEquals(new Integer(4), table.get(1, 1));
  }

  @Test
  public void testRangeH() {
    TableIterator<Integer, Integer, Integer> it = table.range(1);
    table.put(1, 2);
    table.put(1, 1, 4);
    table.put(2, 1, 4);
    it.close();
    it = table.range(1);
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    it.close();
  }

  @Test
  public void testRangeHR() {
    table.put(1, 2);
    table.put(1, 1, 4);
    TableIterator<Integer, Integer, Integer> it = table.range(1, null);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    it.close();
    it = table.range(1, 1);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    table.put(1, 2, 5);
    table.put(2, 2, 5);
    it.close();
    it = table.range(1, 1);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
    it.close();
    it = table.range(1, null);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
    it.close();
  }

  @Test
  public void testRangeHRR() {
    table.put(1, 2);
    table.put(1, 1, 4);
    TableIterator<Integer, Integer, Integer> it = table.range(1, null, 2);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    it.close();
    it = table.range(1, 1, 2);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    table.put(1, 2, 5);
    table.put(1, 3, 5);
    it.close();
    it = table.range(1, 1, 3);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
    it.close();
  }

  @Test
  public void testDeleteH() {
    table.put(1, 1);
    assertEquals(new Integer(1), table.get(1));
    table.delete(1);
    assertEquals(null, table.get(1));
  }

  @Test
  public void testDeleteHR() {
    table.put(1, 1);
    table.put(1, 1, 2);
    assertEquals(new Integer(1), table.get(1));
    assertEquals(new Integer(2), table.get(1, 1));
    table.delete(1, 1);
    assertEquals(new Integer(1), table.get(1));
    assertEquals(null, table.get(1, 1));
  }

  @Test
  public void testSortedStrings() {
    ezdb.deleteTable("test-range-strings");
    RangeTable<Integer, String, Integer> table = ezdb.getTable("test-range-strings", IntegerSerde.get, StringSerde.get, IntegerSerde.get);

    table.put(1213, "20120102-foo", 1);
    table.put(1213, "20120102-bar", 2);
    table.put(1213, "20120101-foo", 3);
    table.put(1213, "20120104-foo", 4);
    table.put(1213, "20120103-foo", 5);
    table.put(1212, "20120102-foo", 1);
    table.put(1214, "20120102-bar", 2);
    table.put(1213, 12345678);

    TableIterator<Integer, String, Integer> it = table.range(1213, "20120102", "20120103");

    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Integer>(1213, "20120102-bar", 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Integer>(1213, "20120102-foo", 1), it.next());
    assertTrue(!it.hasNext());
    it.close();
    assertEquals(new Integer(12345678), table.get(1213));
    table.close();
  }

  @Test
  public void testCustomRangeComparator() {
    RangeTable<Integer, Integer, Integer> table = ezdb.getTable("test-custom-range-comparator", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, new LexicographicalComparator(), new Comparator<byte[]>() {
      // Let's do things in reverse lexicographical order.
      @Override
      public int compare(byte[] o1, byte[] o2) {
        return -1 * ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
      }
    });

    table.put(1, 1, 1);
    table.put(1, 2, 2);
    table.put(1, 3, 3);

    TableIterator<Integer, Integer, Integer> it = table.range(1, 3);

    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 3, 3), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 1), it.next());
    assertTrue(!it.hasNext());
    it.close();
    table.close();
  }

  @Test
  public void testVersionedSortedStrings() {
    ezdb.deleteTable("test-range-strings");
    RangeTable<Integer, String, Versioned<Integer>> table = ezdb.getTable("test-range-strings", IntegerSerde.get, StringSerde.get, new VersionedSerde<Integer>(IntegerSerde.get));

    table.put(1213, "20120102-foo", new Versioned<Integer>(1, 0));
    table.put(1213, "20120102-bar", new Versioned<Integer>(2, 0));
    table.put(1213, "20120102-bar", new Versioned<Integer>(3, 1));
    table.put(1213, "20120101-foo", new Versioned<Integer>(3, 0));
    table.put(1213, "20120104-foo", new Versioned<Integer>(4, 0));
    table.put(1213, "20120103-foo", new Versioned<Integer>(5, 0));
    table.put(1212, "20120102-foo", new Versioned<Integer>(1, 0));
    table.put(1214, "20120102-bar", new Versioned<Integer>(2, 0));
    table.put(1213, new Versioned<Integer>(12345678, 0));

    assertEquals(new Versioned<Integer>(1, 0), table.get(1213, "20120102-foo"));
    assertEquals(new Versioned<Integer>(3, 1), table.get(1213, "20120102-bar"));
    assertEquals(new Versioned<Integer>(12345678, 0), table.get(1213));

    TableIterator<Integer, String, Versioned<Integer>> it = table.range(1213, "20120102", "20120103");

    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Versioned<Integer>>(1213, "20120102-bar", new Versioned<Integer>(3, 1)), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Versioned<Integer>>(1213, "20120102-foo", new Versioned<Integer>(1, 0)), it.next());
    assertTrue(!it.hasNext());
    it.close();
    assertEquals(new Versioned<Integer>(12345678, 0), table.get(1213));

    // check how things work when iterating between null/versioned range keys
    it = table.range(1213);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Versioned<Integer>>(1213, null, new Versioned<Integer>(12345678, 0)), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Versioned<Integer>>(1213, "20120101-foo", new Versioned<Integer>(3, 0)), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Versioned<Integer>>(1213, "20120102-bar", new Versioned<Integer>(3, 1)), it.next());
    // trust that everything works from here on out
    while (it.hasNext()) {
      assertEquals(new Integer(1213), it.next().getHashKey());
    }
    it.close();
    table.close();
  }

  @Before
  public void before() {
    ezdb = new EzLevelDb(new File("/tmp"));
    ezdb.deleteTable("test");
    table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
  }

  @After
  public void after() {
    table.close();
    ezdb.deleteTable("test");
    ezdb.deleteTable("test-simple");
    ezdb.deleteTable("test-range-strings");
    ezdb.deleteTable("test-custom-range-comparator");
    ezdb.deleteTable("test-table-does-not-exist");
  }
}
