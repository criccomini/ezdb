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
	private static final Date MAX_DATE = new Date(9999, 1, 1);
	private static final Date MIN_DATE = new Date(1, 1, 1);
	private final Date now = new Date(2000, 1, 1);
	private final Date oneDate = new Date(now.getTime() + 100000);
	private final Date twoDate = new Date(now.getTime() + 200000);
	private final Date threeDate = new Date(now.getTime() + 300000);

	private final Date oneDatePlus = new Date(oneDate.getTime() + 1);
	private final Date twoDatePlus = new Date(twoDate.getTime() + 1);
	private final Date threeDatePlus = new Date(threeDate.getTime() + 1);
	
	private final Date oneDateMinus = new Date(oneDate.getTime() - 1);
	private final Date twoDateMinus = new Date(twoDate.getTime() - 1);
	private final Date threeDateMinus = new Date(threeDate.getTime() - 1);
	
	private RangeTable<String, Date, Integer> reverseRangeTable;

	@Before
	public void before() {
		FileUtils.deleteRecursively(ROOT);
		ROOT.mkdirs();
		ezdb = new EzLevelDb(ROOT, new EzLevelDbJniFactory());
		ezdb.deleteTable("test");
		table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get,
				IntegerSerde.get);
		Serde<String> hashKeySerde = SerializingSerde.get();
		Serde<Date> hashRangeSerde = SerializingSerde.get();
		Serde<Integer> valueSerde = SerializingSerde.get();

		ezdb.deleteTable("testInverseOrder");
		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put("0", oneDate, -1);
		reverseRangeTable.put("0", twoDate, -2);
		reverseRangeTable.put("0", threeDate, -3);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);
		reverseRangeTable.put("2", oneDate, -10);
		reverseRangeTable.put("2", twoDate, -20);
		reverseRangeTable.put("2", threeDate, -30);
	}

	@Override
	public void after() {
		super.after();
		reverseRangeTable.close();
		ezdb.deleteTable("testInverseOrder");
	}

	@Test
	public void range21Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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

	@Test
	public void range21ReversePlus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDatePlus, oneDatePlus);
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
	
	@Test
	public void range21ReverseMinus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDateMinus, oneDateMinus);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range32Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
	
	@Test
	public void range32ReversePlus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDatePlus, twoDatePlus);
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
	
	@Test
	public void range32ReverseMinus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDateMinus, twoDateMinus);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range12Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, oneDate, twoDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range23Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDate, threeDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range21() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDate, oneDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range32() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, threeDate, twoDate);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range12() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
	
	@Test
	public void range12Plus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, oneDatePlus, twoDatePlus);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void range12Minus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, oneDateMinus, twoDateMinus);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range23() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
	
	@Test
	public void range23Plus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDatePlus, threeDatePlus);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void range23Minus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDateMinus, threeDateMinus);
		Assert.assertEquals(2, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void range2Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
	
	@Test
	public void range2ReversePlus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDatePlus);
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
	
	@Test
	public void range2ReverseMinus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDateMinus);
		Assert.assertEquals(1, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeNoneReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
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

	@Test
	public void range3Reverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
	
	@Test
	public void range3ReversePlus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDatePlus);
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
	
	@Test
	public void range3ReverseMinus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDateMinus);
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

	@Test
	public void range2() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDate);
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
	
	@Test
	public void range2Plus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDatePlus);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void range2Minus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDateMinus);
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

	@Test
	public void rangeNone() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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

	@Test
	public void range3() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, threeDate);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void range3Plus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, threeDatePlus);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void range3Minus() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, threeDateMinus);
		Assert.assertEquals(3, (int) range.next().getValue());
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}
	
	@Test
	public void rangeNow() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, now);
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
	
	@Test
	public void rangeNowReverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, now);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMin() {
		final TableIterator<String, Date, Integer> rangeMin = reverseRangeTable
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

	@Test
	public void rangeMinMax() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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

	@Test
	public void rangeMaxMin() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, MAX_DATE, MIN_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMinReverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, MIN_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMinMaxReverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, MIN_DATE, MAX_DATE);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMaxMinReverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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

	@Test
	public void rangeMaxReverse() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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

	@Test
	public void rangeMax() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
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
