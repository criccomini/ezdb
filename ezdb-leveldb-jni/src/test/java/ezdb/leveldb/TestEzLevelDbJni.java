package ezdb.leveldb;

import java.io.File;
import java.util.Date;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.serde.IntegerSerde;
import ezdb.serde.LongSerde;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;
import ezdb.serde.StringSerde;

public class TestEzLevelDbJni extends TestEzLevelDb {
  @Before
  public void before() {
    ezdb = new EzLevelDb(new File("/tmp"), new EzLevelDbJniFactory());
    ezdb.deleteTable("test");
    table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
  }

  @Test
  public void testInverseOrder() {
    Serde<String> hashKeySerde = SerializingSerde.get();
    Serde<Date> hashRangeSerde = SerializingSerde.get();
    Serde<Integer> valueSerde = SerializingSerde.get();
    ezdb.deleteTable("testInverseOrder");
    final RangeTable<String, Date, Integer> rangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
        hashRangeSerde, valueSerde);
    final Date now = new Date();
    final Date oneDate = new Date(now.getTime() + 10000);
    rangeTable.put("one", oneDate, 1);
    final Date twoDate = new Date(now.getTime() + 20000);
    rangeTable.put("one", twoDate, 2);
    final Date threeDate = new Date(now.getTime() + 30000);
    rangeTable.put("one", threeDate, 3);
    final TableIterator<String, Date, Integer> range3 = rangeTable.range("one", now);
    Assert.assertEquals(1, (int) range3.next().getValue());
    Assert.assertEquals(2, (int) range3.next().getValue());
    Assert.assertEquals(3, (int) range3.next().getValue());
    Assert.assertFalse(range3.hasNext());
    try {
      range3.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> rangeNone = rangeTable.range("one");
    Assert.assertEquals(1, (int) rangeNone.next().getValue());
    Assert.assertEquals(2, (int) rangeNone.next().getValue());
    Assert.assertEquals(3, (int) rangeNone.next().getValue());
    Assert.assertFalse(rangeNone.hasNext());
    try {
      rangeNone.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> range2 = rangeTable.range("one", twoDate);
    Assert.assertEquals(2, (int) range2.next().getValue());
    Assert.assertEquals(3, (int) range2.next().getValue());
    Assert.assertFalse(range2.hasNext());
    try {
      range2.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> range3Reverse = rangeTable.rangeReverse("one", threeDate);
    Assert.assertEquals(3, (int) range3Reverse.next().getValue());
    Assert.assertEquals(2, (int) range3Reverse.next().getValue());
    Assert.assertEquals(1, (int) range3Reverse.next().getValue());
    Assert.assertFalse(range3Reverse.hasNext());
    try {
      range3Reverse.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> rangeNoneReverse = rangeTable.rangeReverse("one");
    Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
    Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
    Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
    Assert.assertFalse(rangeNoneReverse.hasNext());
    try {
      rangeNoneReverse.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> range2Reverse = rangeTable.rangeReverse("one", twoDate);
    Assert.assertEquals(2, (int) range2Reverse.next().getValue());
    Assert.assertEquals(1, (int) range2Reverse.next().getValue());
    Assert.assertFalse(range2Reverse.hasNext());
    try {
      range2Reverse.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> range32Reverse = rangeTable.rangeReverse("one", threeDate, twoDate);
    Assert.assertEquals(3, (int) range32Reverse.next().getValue());
    Assert.assertEquals(2, (int) range32Reverse.next().getValue());
    Assert.assertFalse(range32Reverse.hasNext());
    try {
      range32Reverse.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }

    final TableIterator<String, Date, Integer> range21Reverse = rangeTable.rangeReverse("one", twoDate, oneDate);
    Assert.assertEquals(2, (int) range21Reverse.next().getValue());
    Assert.assertEquals(1, (int) range21Reverse.next().getValue());
    Assert.assertFalse(range21Reverse.hasNext());
    try {
      range21Reverse.next();
      Assert.fail("Exception expected!");
    } catch (final NoSuchElementException e) {
      Assert.assertNotNull(e);
    }
  }

}
