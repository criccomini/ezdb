package ezdb.leveldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import junit.framework.Assert;

import org.iq80.leveldb.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.CharStreams;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.comparator.SerdeComparator;
import ezdb.serde.DateSerde;
import ezdb.serde.IntegerSerde;
import ezdb.serde.LongSerde;
import ezdb.serde.Serde;
import ezdb.serde.SerializingSerde;
import ezdb.serde.StringSerde;

public class TestStockData {

	private static final String MSFT = "MSFT";
	private static final Date MAX_DATE = new GregorianCalendar(5555, 1, 1)
			.getTime();
	private static final Date MIN_DATE = new GregorianCalendar(1, 1, 1)
			.getTime();

	private final Serde<String> hashKeySerde = StringSerde.get;
	private final Serde<Date> hashRangeSerde = DateSerde.get;
	private final Serde<Integer> valueSerde = SerializingSerde.get();

	protected static final File ROOT = FileUtils
			.createTempDir(TestEzLevelDb.class.getSimpleName());
	protected Db ezdb;
	protected RangeTable<String, Date, Integer> table;
	private Comparator<byte[]> hashKeyComparator = new SerdeComparator<String>(
			hashKeySerde);
	private Comparator<byte[]> rangeKeyComparator = new SerdeComparator<Date>(
			hashRangeSerde);

	@Before
	public void before() {
		FileUtils.deleteRecursively(ROOT);
		ROOT.mkdirs();
		ezdb = new EzLevelDb(ROOT, new EzLevelDbJniFactory());
		ezdb.deleteTable("test");
		table = ezdb.getTable("test", hashKeySerde, hashRangeSerde, valueSerde,
				hashKeyComparator, rangeKeyComparator);
	}

	@After
	public void after() {
		table.close();
		ezdb.deleteTable("test");
		FileUtils.deleteRecursively(ROOT);
	}

	@Test
	public void testStockData() throws IOException, ParseException {
		FileInputStream in = new FileInputStream(new File(
				"./src/test/java/ezdb/leveldb/" + MSFT + ".txt"));
		List<String> lines = CharStreams.readLines(new InputStreamReader(in));
		lines.remove(0);
		lines.remove(0);
		Collections.reverse(lines);
		in.close();
		Long prevLongTime = null;
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		int countDates = 0;
		Date firstDate = null;
		Date lastDate = null;
		for (String line : lines) {
			String[] split = line.split(",");
			Assert.assertEquals(7, split.length);
			countDates++;
			String dateStr = split[0];
			Date date = df.parse(dateStr);
			if (firstDate == null) {
				firstDate = date;
			}
			lastDate = date;
			long longTime = date.getTime();
			if (prevLongTime != null) {
				// System.out.println(dateStr + ":"+date +
				// " - "+prevLongTime+"  < " + longTime + " -> "
				// + (prevLongTime < longTime));
				Assert.assertTrue(prevLongTime < longTime);
			}
			table.put(MSFT, date, countDates);
			prevLongTime = longTime;
		}
		System.out.println(MSFT + " has " + countDates + " bars");

		assertIteration(countDates, MIN_DATE, MAX_DATE);
		assertIteration(countDates, firstDate, lastDate);
	}

	private void assertIteration(int countDates, Date fromDate, Date toDate) {
		TableIterator<String, Date, Integer> range = table.range(MSFT,
				fromDate, toDate);
		int iteratedBars = 0;
		int prevValue = 0;
		Date left1000Date = null;
		Date left900Date = null;
		while (range.hasNext()) {
			TableRow<String, Date, Integer> next = range.next();
			Integer value = next.getValue();
			// System.out.println(value);
			iteratedBars++;
			Assert.assertTrue(prevValue < value);
			prevValue = value;
			if (iteratedBars == countDates - 999) {
				left1000Date = next.getRangeKey();
			}
			if (iteratedBars == countDates - 900) {
				left900Date = next.getRangeKey();
			}
		}
		Assert.assertEquals(countDates, iteratedBars);

		Assert.assertEquals((Integer) 1, table.getLatest(MSFT, fromDate));

		Assert.assertEquals((Integer) countDates, table.getLatest(MSFT, toDate));

//		System.out.println(left1000Date +" -> "+left900Date);
		range = table.range(MSFT, left1000Date, left900Date);
		int curLeftIt = 0;
		while (range.hasNext()) {
			TableRow<String, Date, Integer> next = range.next();
			curLeftIt++;
			Assert.assertEquals((Integer) (countDates - 1000 + curLeftIt),
					next.getValue());
		}
		Assert.assertEquals(100, curLeftIt);
	}

}
