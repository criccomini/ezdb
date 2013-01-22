package ezdb.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ezdb.Db;
import ezdb.Table;
import ezdb.TableIterator;
import ezdb.serde.IntegerSerde;
import ezdb.serde.StringSerde;

public class TestEzLevelDbTable {
  private Db ezdb;
  private Table<Integer, Integer, Integer> table;

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
    table.put(1, 1);
    assertEquals(new Integer(1), table.get(1));
    table.put(1, 2);
    assertEquals(new Integer(2), table.get(1));
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
    Table<Integer, String, Integer> table = ezdb.getTable("test-range-strings", IntegerSerde.get, StringSerde.get, IntegerSerde.get);

    table.put(1213, "20120102-foo", 1);
    table.put(1213, "20120102-bar", 2);
    table.put(1213, "20120101-foo", 3);
    table.put(1213, "20120104-foo", 4);
    table.put(1213, "20120103-foo", 5);
    table.put(1212, "20120102-foo", 1);
    table.put(1214, "20120102-bar", 2);

    TableIterator<Integer, String, Integer> it = table.range(1213, "20120102", "20120103");

    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Integer>(1213, "20120102-bar", 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, String, Integer>(1213, "20120102-foo", 1), it.next());
    assertTrue(!it.hasNext());
    it.close();
  }

  @Before
  public void before() {
    ezdb = new EzLevelDb(new File("/tmp"));
    ezdb.deleteTable("test");
    table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
  }

  @After
  public void after() {
    ezdb.deleteTable("test");
  }
}
