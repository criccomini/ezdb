package ezdb.lmdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Ignore;
import org.junit.Test;

import ezdb.TableRow;
import ezdb.comparator.LexicographicalComparator;
import ezdb.lmdb.util.FileUtils;
import ezdb.serde.LongSerde;
import ezdb.serde.StringSerde;

@NotThreadSafe
@Ignore("manual test")
public class LmdbPerformanceTest extends ADatabasePerformanceTest {

	protected static final File ROOT = FileUtils.createTempDir(TestEzLmDb.class.getSimpleName());

	@Test
	public void testLevelDbPerformance() {
		final EzLmDbTable<String, Long, Long> table = new EzLmDbTable(ROOT, new EzLmDbJnrFactory(), StringSerde.get,
				LongSerde.get, LongSerde.get, new LexicographicalComparator(), new LexicographicalComparator());

		// RangeBatch<String, FDate, FDate> batch = table.newRangeBatch();
		final long writesStart = System.currentTimeMillis();
		for (long i = 0; i < VALUES; i++) {
			table.put(HASH_KEY, i, i);
			if (i % FLUSH_INTERVAL == 0) {
				printProgress("Writes", writesStart, i, VALUES);
				// try {
				// batch.flush();
				// batch.close();
				// } catch (final IOException e) {
				// throw new RuntimeException(e);
				// }
				// batch = table.newRangeBatch();
			}
		}
		// try {
		// batch.flush();
		// batch.close();
		// } catch (final IOException e) {
		// throw new RuntimeException(e);
		// }
		printProgress("WritesFinished", writesStart, VALUES, VALUES);

		readIterator(table);
		readGet(table);
		readGetLatest(table);
	}

	private void readIterator(final EzLmDbTable<String, Long, Long> table) {
		final long readsStart = System.currentTimeMillis();
		for (int reads = 1; reads <= READS; reads++) {
			Long prevValue = null;
			final Iterator<TableRow<String, Long, Long>> range = table.range(HASH_KEY);
			int count = 0;
			while (true) {
				try {
					final TableRow<String, Long, Long> value = range.next();
//					if (prevValue != null) {
//						if (prevValue >= value.getValue()) {
//							throw new IllegalStateException(prevValue + " >= " + value.getValue());
//						}
//					}
					prevValue = value.getValue();
					count++;
				} catch (final NoSuchElementException e) {
					break;
				}
			}
			printProgress("Reads", readsStart, VALUES * reads, VALUES * READS);
		}
		printProgress("ReadsFinished", readsStart, VALUES * READS, VALUES * READS);
	}

	private void readGet(final EzLmDbTable<String, Long, Long> table) {
		final List<Long> values = newValues();
		final long readsStart = System.currentTimeMillis();
		for (int reads = 1; reads <= READS; reads++) {
			Long prevValue = null;
			for (int i = 0; i < values.size(); i++) {
				try {
					final Long value = table.get(HASH_KEY, values.get(i));
//					if (prevValue != null) {
//						if (prevValue >= value) {
//							throw new IllegalStateException(prevValue + " >= " + value.getValue());
//						}
//					}
					prevValue = value;
				} catch (final NoSuchElementException e) {
					break;
				}
			}
			printProgress("Gets", readsStart, VALUES * reads, VALUES * READS);
		}
		printProgress("GetsFinished", readsStart, VALUES * READS, VALUES * READS);
	}

	private List<Long> newValues() {
		final List<Long> values = new ArrayList<Long>();
		for (long i = 0; i < VALUES; i++) {
			values.add(i);
		}
		return values;
	}

	private void readGetLatest(final EzLmDbTable<String, Long, Long> table) {
		final List<Long> values = newValues();
		final long readsStart = System.currentTimeMillis();
		for (int reads = 1; reads <= READS; reads++) {
			Long prevValue = null;
			for (int i = 0; i < values.size(); i++) {
				try {
					final TableRow<String, Long, Long> value = table.getLatest(HASH_KEY, values.get(i));
//					if (prevValue != null) {
//						if (prevValue >= value.getValue()) {
//							throw new IllegalStateException(prevValue + " >= " + value.getValue());
//						}
//					}
					prevValue = value.getValue();
				} catch (final NoSuchElementException e) {
					break;
				}
			}
			printProgress("GetLatests", readsStart, VALUES * reads, VALUES * READS);
		}
		printProgress("GetLatestsFinished", readsStart, VALUES * READS, VALUES * READS);
	}

}
