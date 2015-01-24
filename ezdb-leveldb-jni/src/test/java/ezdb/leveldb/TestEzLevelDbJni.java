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
	private static final String HASHKEY_ONE = "1";
	private static final Date MAX_DATE = new Date(9999, 1 ,1);
	private static final Date MIN_DATE = new Date(1, 1 ,1);
	private final Date now = new Date();
	private final Date oneDate = new Date(now.getTime() + 100000);
	private final Date twoDate = new Date(now.getTime() + 200000);
	private final Date threeDate = new Date(now.getTime() + 300000);

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
		
		rangeTable.put("0", oneDate, -1);
		rangeTable.put("0", twoDate, -2);
		rangeTable.put("0", threeDate, -3);
		rangeTable.put(HASHKEY_ONE, oneDate, 1);
		rangeTable.put(HASHKEY_ONE, twoDate, 2);
		rangeTable.put(HASHKEY_ONE, threeDate, 3);
		rangeTable.put("2", oneDate, -10);
		rangeTable.put("2", twoDate, -20);
		rangeTable.put("2", threeDate, -30);
		
		System.out.println("range3");
		range3(rangeTable);

		System.out.println("rangeNone");
		rangeNone(rangeTable);

		System.out.println("range2");
		range2(rangeTable);
		
		System.out.println("range21");
		range21(rangeTable);
		
		System.out.println("range32");
		range32(rangeTable);
		
		System.out.println("range12");
		range12(rangeTable);
		
		System.out.println("range23");
		range23(rangeTable);
		
		System.out.println("rangeMin");
		rangeMin(rangeTable);
		
		System.out.println("rangeMax");
		rangeMax(rangeTable);
		
		System.out.println("rangeMinMax");
		rangeMinMax(rangeTable);
		
		System.out.println("rangeMaxMin");
		rangeMaxMin(rangeTable);
		
		System.out.println("range12Reverse");
		range12Reverse(rangeTable);

		System.out.println("range23Reverse");
		range23Reverse(rangeTable);
		
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
				.rangeReverse(HASHKEY_ONE, twoDate, oneDate);
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
				.rangeReverse(HASHKEY_ONE, threeDate, twoDate);
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
	
	private void range12Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY_ONE, oneDate, twoDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range23Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY_ONE, twoDate, threeDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void range21(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY_ONE, twoDate, oneDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range32(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY_ONE, threeDate, twoDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	private void range12(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY_ONE, oneDate, twoDate);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	private void range23(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.range(HASHKEY_ONE, twoDate, threeDate);
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


	private void range2Reverse(
			final RangeTable<String, Date, Integer> rangeTable) {
		final TableIterator<String, Date, Integer> range = rangeTable
				.rangeReverse(HASHKEY_ONE, twoDate);
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
				.rangeReverse(HASHKEY_ONE);
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
				.rangeReverse(HASHKEY_ONE, threeDate);
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
				HASHKEY_ONE, twoDate);
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
				.range(HASHKEY_ONE);
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
				HASHKEY_ONE, now);
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
				.range(HASHKEY_ONE, MIN_DATE);
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
				.range(HASHKEY_ONE, MIN_DATE, MAX_DATE);
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
				.range(HASHKEY_ONE, MAX_DATE, MIN_DATE);
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
				.rangeReverse(HASHKEY_ONE, MIN_DATE);
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
				.rangeReverse(HASHKEY_ONE, MIN_DATE, MAX_DATE);
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
				.rangeReverse(HASHKEY_ONE, MAX_DATE, MIN_DATE);
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
				.rangeReverse(HASHKEY_ONE, MAX_DATE);
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
				.range(HASHKEY_ONE, MAX_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}


}
