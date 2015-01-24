package ezdb.leveldb;

import java.io.File;
import java.util.Date;
import java.util.NoSuchElementException;

import org.iq80.leveldb.util.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;

public class TestEzLevelDbJni extends TestEzLevelDb {
	private static final String HASHKEY = "1";
	private static final Date MAX_DATE = new Date(9999, 1 ,1);
	private static final Date MIN_DATE = new Date(1, 1 ,1);
	final Date now = new Date();
	final Date oneDate = new Date(now.getTime() + 100000);
	final Date twoDate = new Date(now.getTime() + 200000);
	final Date threeDate = new Date(now.getTime() + 300000);

	@Before
	public void before() {
		FileUtils.deleteRecursively(ROOT);
		ROOT.mkdirs();
		ezdb = new EzLevelDb(ROOT, new EzLevelDbJniFactory());
		ezdb.deleteTable("test");
		table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get,
				IntegerSerde.get);
	}

	@Test
	public void testInverseOrder() {
		Serde<String> hashKeySerde = SerializingSerde.get();
		Serde<Date> hashRangeSerde = SerializingSerde.get();
		Serde<Integer> valueSerde = SerializingSerde.get();
		ezdb.deleteTable("testInverseOrder");
		final RangeTable<String, Date, Integer> rangeTable = ezdb.getTable(
				"testInverseOrder", hashKeySerde, hashRangeSerde, valueSerde);
		//TODO: fix surrounding values!!!
//		rangeTable.put("0", oneDate, -1);
//		rangeTable.put("0", twoDate, -2);
//		rangeTable.put("0", threeDate, -3);
		rangeTable.put(HASHKEY, oneDate, 1);
		rangeTable.put(HASHKEY, twoDate, 2);
		rangeTable.put(HASHKEY, threeDate, 3);
//		rangeTable.put("1", oneDate, -10);
//		rangeTable.put("1", twoDate, -20);
//		rangeTable.put("1", threeDate, -30);
		
		System.out.println("range3");
		range3(rangeTable);

		System.out.println("rangeNone");
		rangeNone(rangeTable);

		System.out.println("range2");
		range2(rangeTable);

		System.out.println("range3Reverse");
		range3Reverse(rangeTable);

		System.out.println("rangeNoneReverse");
		rangeNoneReverse(rangeTable);

		System.out.println("range2Reverse");
		range2Reverse(rangeTable);

		System.out.println("range32Reverse");
		range32Reverse(rangeTable);

		System.out.println("range21Reverse");
		range21Reverse(rangeTable);
		
		System.out.println("rangeMin");
		rangeMin(rangeTable);
		
		System.out.println("rangeMax");
		rangeMax(rangeTable);
		
		System.out.println("rangeMinMax");
		rangeMinMax(rangeTable);
		
		System.out.println("rangeMaxMin");
		rangeMaxMin(rangeTable);
		
		System.out.println("rangeMinReverse");
		rangeMinReverse(rangeTable);
		
		System.out.println("rangeMinMaxReverse");
		rangeMinMaxReverse(rangeTable);
		
		System.out.println("rangeMaxReverse");
		rangeMaxReverse(rangeTable);
		
		System.out.println("rangeMaxMinReverse");
		rangeMaxMinReverse(rangeTable);
		
		
	}

	private void range21Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, twoDate, oneDate);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range32Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, threeDate, twoDate);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range2Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, twoDate);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void rangeNoneReverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> rangeNoneReverse = rangeTable
				.rangeReverse(HASHKEY);
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
	}

	private void range3Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, threeDate);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range2(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable.range(
				HASHKEY, twoDate);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void rangeNone(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range3(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable.range(
				HASHKEY, now);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMin(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> rangeMin = rangeTable
				.range(HASHKEY, MIN_DATE);
		Assert.assertEquals(1, (int) rangeMin.next().getValue());
		Assert.assertEquals(2, (int) rangeMin.next().getValue());
		Assert.assertEquals(3, (int) rangeMin.next().getValue());
		Assert.assertFalse(rangeMin.hasNext());
		try {
			rangeMin.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMinMax(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY, MIN_DATE, MAX_DATE);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMaxMin(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY, MAX_DATE, MIN_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMinReverse(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, MIN_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMinMaxReverse(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, MIN_DATE, MAX_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMaxMinReverse(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, MAX_DATE, MIN_DATE);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void rangeMaxReverse(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY, MAX_DATE);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void rangeMax(final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY, MAX_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}


}
