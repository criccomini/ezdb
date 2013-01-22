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

public class TestLevelDbTable {
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
    it = table.range(1);
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
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
    it = table.range(1, 1);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    table.put(1, 2, 5);
    it = table.range(1, 1);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
    it = table.range(1, null);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
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
    it = table.range(1, 1, 2);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(!it.hasNext());
    table.put(1, 2, 5);
    table.put(1, 3, 5);
    it = table.range(1, 1, 2);
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
    assertTrue(it.hasNext());
    assertEquals(new EzLevelDbTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
    assertTrue(!it.hasNext());
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
