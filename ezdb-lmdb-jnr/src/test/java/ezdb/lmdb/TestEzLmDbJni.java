package ezdb.lmdb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.lmdb.EzLmDb;
import ezdb.lmdb.EzLmDbJnrFactory;
import ezdb.lmdb.util.FileUtils;
import ezdb.serde.IntegerSerde;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;

public class TestEzLmDbJni extends TestEzLmDb {
	private static final String HASHKEY_ONE = "1";
	private static final Date MAX_DATE = new GregorianCalendar(5555, 1, 1)
			.getTime();
	private static final Date MIN_DATE = new GregorianCalendar(1, 1, 1)
			.getTime();
	private final Date now = new GregorianCalendar(2000, 1, 1).getTime();
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
	private final Serde<String> hashKeySerde = SerializingSerde.get();
	private final Serde<Date> hashRangeSerde = SerializingSerde.get();
	private final Serde<Integer> valueSerde = SerializingSerde.get();

	@Before
	public void before() {
		FileUtils.deleteRecursively(ROOT);
		ROOT.mkdirs();
		ezdb = new EzLmDb(ROOT, new EzLmDbJnrFactory());
		ezdb.deleteTable("test");
		table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get,
				IntegerSerde.get);

		ezdb.deleteTable("testInverseOrder");
		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);
	}

	@Override
	public void after() {
		super.after();
		clearTable();
	}

	private void clearTable() {
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
	public void rangeNullReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, null);
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
	public void rangeNullNullReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, null, null);
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
	public void range2NullReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDate, null);
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
	public void rangeNull2Reverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, null, twoDate);
		Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
		Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
		Assert.assertFalse(rangeNoneReverse.hasNext());
		try {
			rangeNoneReverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMaxNullReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, MAX_DATE, null);
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
	public void rangeNullMaxReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, null, MAX_DATE);
		Assert.assertFalse(rangeNoneReverse.hasNext());
		try {
			rangeNoneReverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeMinNullReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, MIN_DATE, null);
		Assert.assertFalse(rangeNoneReverse.hasNext());
		try {
			rangeNoneReverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeNullMinReverse() {
		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, null, MIN_DATE);
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
	public void rangeNull() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, null);
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
	public void rangeNullNull() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, null, null);
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
	public void range2Null() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, twoDate, null);
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
	public void rangeNull2() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, null, twoDate);
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
	public void rangeMaxNull() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, MAX_DATE, null);
		Assert.assertFalse(range.hasNext());
		try {
			range.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

	@Test
	public void rangeNullMax() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, null, MAX_DATE);
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
	public void rangeMinNull() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, MIN_DATE, null);
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
	public void rangeNullMin() {
		final TableIterator<String, Date, Integer> range = reverseRangeTable
				.range(HASHKEY_ONE, null, MIN_DATE);
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

	@Test
	public void getNone() {
		Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE));
	}

	@Test
	public void getNull() {
		Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, null));
	}

	@Test
	public void get2() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.get(HASHKEY_ONE, twoDate));
	}

	@Test
	public void getMin() {
		Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, MIN_DATE));
	}

	@Test
	public void getMax() {
		Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, MAX_DATE));
	}

	@Test
	public void get2Plus() {
		Assert.assertEquals(null,
				reverseRangeTable.get(HASHKEY_ONE, twoDatePlus));
	}

	@Test
	public void get2Minus() {
		Assert.assertEquals(null,
				reverseRangeTable.get(HASHKEY_ONE, twoDateMinus));
	}

	@Test
	public void getLastNone() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE).getValue());
	}

	@Test
	public void getLastNull() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE, null).getValue());
	}

	@Test
	public void getLast2() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getLatest(HASHKEY_ONE, twoDate).getValue());
	}

	@Test
	public void getLastMin() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, MIN_DATE).getValue());
	}

	@Test
	public void getLastMax() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE, MAX_DATE).getValue());
	}

	@Test
	public void getLast2Plus() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getLatest(HASHKEY_ONE, twoDatePlus)
						.getValue());
	}

	@Test
	public void getLast2Minus() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, twoDateMinus)
						.getValue());
	}

	@Test
	public void getNext2() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getNext(HASHKEY_ONE, twoDate).getValue());
	}

	@Test
	public void getNext2Minus() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getNext(HASHKEY_ONE, twoDateMinus).getValue());
	}

	@Test
	public void getNext2Plus() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getNext(HASHKEY_ONE, twoDatePlus).getValue());
	}

	@Test
	public void getNextNull() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getNext(HASHKEY_ONE, null).getValue());
	}

	@Test
	public void getNextMin() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getNext(HASHKEY_ONE, MIN_DATE).getValue());
	}

	@Test
	public void getNextMax() {
		Assert.assertEquals(null,
				reverseRangeTable.getNext(HASHKEY_ONE, MAX_DATE));
	}

	@Test
	public void getPrev2() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getPrev(HASHKEY_ONE, twoDate).getValue());
	}

	@Test
	public void getPrev2Minus() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getPrev(HASHKEY_ONE, twoDateMinus).getValue());
	}

	@Test
	public void getPrev2Plus() {
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getPrev(HASHKEY_ONE, twoDatePlus).getValue());
	}

	@Test
	public void getPrevNull() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getPrev(HASHKEY_ONE, null).getValue());
	}

	@Test
	public void getPrevMin() {
		Assert.assertEquals(null,
				reverseRangeTable.getPrev(HASHKEY_ONE, MIN_DATE));
	}

	@Test
	public void getPrevMax() {
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getPrev(HASHKEY_ONE, MAX_DATE).getValue());
	}

	@Test
	public void testVariationsOfDatasetNormal()
			throws IllegalArgumentException, IllegalAccessException {
		for (Method m : getClass().getDeclaredMethods()) {
			try {
				if (m.getAnnotation(Test.class) != null
						&& !m.getName().startsWith("testVariationsOfDataset")) {
					// System.out.println(m.getName());
					m.invoke(this);
				}
			} catch (InvocationTargetException t) {
				throw new RuntimeException("at: " + m.getName(),
						t.getTargetException());
			}
		}
	}

	@Test
	public void testVariationsOfDataset012() throws IllegalArgumentException,
			IllegalAccessException {
		clearTable();

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

		testVariationsOfDatasetNormal();
	}

	@Test
	public void testVariationsOfDataset01() throws IllegalArgumentException,
			IllegalAccessException {
		clearTable();

		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put("0", oneDate, -1);
		reverseRangeTable.put("0", twoDate, -2);
		reverseRangeTable.put("0", threeDate, -3);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);

		testVariationsOfDatasetNormal();
	}

	@Test
	public void testVariationsOfDataset12() throws IllegalArgumentException,
			IllegalAccessException {
		clearTable();

		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);
		reverseRangeTable.put("2", oneDate, -10);
		reverseRangeTable.put("2", twoDate, -20);
		reverseRangeTable.put("2", threeDate, -30);

		testVariationsOfDatasetNormal();
	}

	@Test
	public void testVariationsOfDataset210Reverse()
			throws IllegalArgumentException, IllegalAccessException {
		clearTable();

		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put("2", threeDate, -30);
		reverseRangeTable.put("2", twoDate, -20);
		reverseRangeTable.put("2", oneDate, -10);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put("0", threeDate, -3);
		reverseRangeTable.put("0", twoDate, -2);
		reverseRangeTable.put("0", oneDate, -1);

		testVariationsOfDatasetNormal();
	}

	@Test
	public void testVariationsOfDataset210() throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		clearTable();

		reverseRangeTable = ezdb.getTable("testInverseOrder", hashKeySerde,
				hashRangeSerde, valueSerde);
		reverseRangeTable.put("2", oneDate, -10);
		reverseRangeTable.put("2", twoDate, -20);
		reverseRangeTable.put("2", threeDate, -30);
		reverseRangeTable.put(HASHKEY_ONE, oneDate, 1);
		reverseRangeTable.put(HASHKEY_ONE, twoDate, 2);
		reverseRangeTable.put(HASHKEY_ONE, threeDate, 3);
		reverseRangeTable.put("0", oneDate, -1);
		reverseRangeTable.put("0", twoDate, -2);
		reverseRangeTable.put("0", threeDate, -3);

		testVariationsOfDatasetNormal();
	}

	@Test
	public void testInverseOrder() {
		final TableIterator<String, Date, Integer> range3 = reverseRangeTable
				.range(HASHKEY_ONE, now);
		Assert.assertEquals((Integer) 1, range3.next().getValue());
		Assert.assertEquals((Integer) 2, range3.next().getValue());
		Assert.assertEquals((Integer) 3, range3.next().getValue());
		Assert.assertFalse(range3.hasNext());
		try {
			range3.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
		range3.close(); // should already be closed but should not cause an
						// error when calling again

		final TableIterator<String, Date, Integer> rangeNone = reverseRangeTable
				.range(HASHKEY_ONE);
		Assert.assertEquals((Integer) 1, rangeNone.next().getValue());
		Assert.assertEquals((Integer) 2, rangeNone.next().getValue());
		Assert.assertEquals((Integer) 3, rangeNone.next().getValue());
		Assert.assertFalse(rangeNone.hasNext());
		try {
			rangeNone.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> rangeMin = reverseRangeTable
				.range(HASHKEY_ONE, MIN_DATE);
		Assert.assertEquals((Integer) 1, rangeMin.next().getValue());
		Assert.assertEquals((Integer) 2, rangeMin.next().getValue());
		Assert.assertEquals((Integer) 3, rangeMin.next().getValue());
		Assert.assertFalse(rangeMin.hasNext());
		try {
			rangeMin.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> rangeMax = reverseRangeTable
				.range(HASHKEY_ONE, MAX_DATE);
		Assert.assertFalse(rangeMax.hasNext());
		try {
			rangeMax.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> range2 = reverseRangeTable
				.range(HASHKEY_ONE, twoDate);
		Assert.assertEquals((Integer) 2, range2.next().getValue());
		Assert.assertEquals((Integer) 3, range2.next().getValue());
		Assert.assertFalse(range2.hasNext());
		try {
			range2.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		testReverse();

		testGetLatestForRange();
	}

	private void testGetLatestForRange() {
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, oneDate).getValue());
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getLatest(HASHKEY_ONE, twoDate).getValue());
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE, threeDate).getValue());

		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, oneDateMinus)
						.getValue());
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, twoDateMinus)
						.getValue());
		Assert.assertEquals((Integer) 2,
				reverseRangeTable.getLatest(HASHKEY_ONE, threeDateMinus)
						.getValue());
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE, threeDatePlus)
						.getValue());
		Assert.assertEquals(
				(Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE,
						new Date(threeDatePlus.getTime() + 1000)).getValue());
		Assert.assertEquals((Integer) 1,
				reverseRangeTable.getLatest(HASHKEY_ONE, MIN_DATE).getValue());
		Assert.assertEquals((Integer) 3,
				reverseRangeTable.getLatest(HASHKEY_ONE, MAX_DATE).getValue());
	}

	private void testReverse() {
		final TableIterator<String, Date, Integer> range3Reverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDate);
		Assert.assertEquals((Integer) 3, range3Reverse.next().getValue());
		Assert.assertEquals((Integer) 2, range3Reverse.next().getValue());
		Assert.assertEquals((Integer) 1, range3Reverse.next().getValue());
		Assert.assertFalse(range3Reverse.hasNext());
		try {
			range3Reverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> rangeNoneReverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE);
		Assert.assertEquals((Integer) 3, rangeNoneReverse.next().getValue());
		Assert.assertEquals((Integer) 2, rangeNoneReverse.next().getValue());
		Assert.assertEquals((Integer) 1, rangeNoneReverse.next().getValue());
		Assert.assertFalse(rangeNoneReverse.hasNext());
		try {
			rangeNoneReverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> range2Reverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDate);
		Assert.assertEquals((Integer) 2, range2Reverse.next().getValue());
		Assert.assertEquals((Integer) 1, range2Reverse.next().getValue());
		Assert.assertFalse(range2Reverse.hasNext());
		try {
			range2Reverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> range32Reverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, threeDate, twoDate);
		Assert.assertEquals((Integer) 3, range32Reverse.next().getValue());
		Assert.assertEquals((Integer) 2, range32Reverse.next().getValue());
		Assert.assertFalse(range32Reverse.hasNext());
		try {
			range32Reverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}

		final TableIterator<String, Date, Integer> range21Reverse = reverseRangeTable
				.rangeReverse(HASHKEY_ONE, twoDate, oneDate);
		Assert.assertEquals((Integer) 2, range21Reverse.next().getValue());
		Assert.assertEquals((Integer) 1, range21Reverse.next().getValue());
		Assert.assertFalse(range21Reverse.hasNext());
		try {
			range21Reverse.next();
			Assert.fail("Exception expected!");
		} catch (final NoSuchElementException e) {
			Assert.assertNotNull(e);
		}
	}

}
