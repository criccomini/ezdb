package ezdb.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import ezdb.Db;
import ezdb.comparator.LexicographicalComparator;
import ezdb.serde.IntegerSerde;
import ezdb.table.RangeTableRow;
import ezdb.table.range.RangeTable;
import ezdb.treemap.bytes.table.range.BytesTreeMapRangeTable;
import ezdb.util.TableIterator;

/**
 * This is a little test that mostly just compares random behavior between a
 * mock implementation, and LevelDB. It's not really a valid performance test,
 * since it resets the tables frequently enough to never get a large data set,
 * and duplicates all actions in TreeMap. It does, however, test
 * multi-threading.
 * 
 * @author criccomini
 * 
 */
public class TestEzLevelDbTorture {
	public static final int NUM_THREADS = 10;
	public static final int ITERATIONS = 50000;
	public static final String tableName = "torture";

	public Db<ByteBuffer> db;

	@Before
	public void before() {
		db = new EzLevelDbJava(new File("/tmp"), newFactory());
	}

	protected EzLevelDbJavaFactory newFactory() {
		return new EzLevelDbJavaFactory();
	}

	@Test
	public void testTortureEzLevelDb() throws InterruptedException {
		db.deleteTable(tableName);
		final Thread[] threads = new Thread[NUM_THREADS];

		for (int i = 0; i < NUM_THREADS; ++i) {
			threads[i] = new Thread(new TortureRunnable(i, db));
			threads[i].start();
		}

		for (int i = 0; i < NUM_THREADS; ++i) {
			threads[i].join();
		}
	}

	public static class TortureRunnable implements Runnable {
		private final int offset;
		private final Db<ByteBuffer> db;

		public TortureRunnable(final int threadId, final Db<ByteBuffer> db) {
			this.offset = threadId * 1000;
			this.db = db;
		}

		@Override
		public void run() {
			final Random rand = new Random();
			final RangeTable<Integer, Integer, Integer> table = db.getRangeTable(tableName, IntegerSerde.get,
					IntegerSerde.get, IntegerSerde.get);
			final RangeTable<Integer, Integer, Integer> mockTable = new BytesTreeMapRangeTable<Integer, Integer, Integer>(
					IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, LexicographicalComparator.get,
					LexicographicalComparator.get);
			final long tables = 0;
			long deletes = 0, writes = 0, reads = 0, rangeH = 0, rangeHF = 0, rangeHFT = 0;
			final long start = System.currentTimeMillis();

			for (int i = 0; i < ITERATIONS; ++i) {
				// pick something to do
				final double pick = rand.nextDouble();
				final int hashKey = rand.nextInt(500) + offset;
				final int rangeKey = rand.nextInt(500);
				final int value = rand.nextInt(500);

				if (pick < 0.1) {
					// 10% of the time, delete
					table.delete(hashKey, rangeKey);
					mockTable.delete(hashKey, rangeKey);
					++deletes;
				} else if (pick < 0.30) {
					// 20% of the time write
					table.put(hashKey, rangeKey, value);
					mockTable.put(hashKey, rangeKey, value);
					++writes;
				} else if (pick < 0.60) {
					// 30% of the time read
					assertEquals(mockTable.get(hashKey, rangeKey), table.get(hashKey, rangeKey));
					++reads;
				} else if (pick < 0.70) {
					// 10% of the time range(h)
					final int hash = rand.nextInt(500) + offset;
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> iterator = table.range(hash);
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> mockIterator = table.range(hash);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeH;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
					iterator.close();
					mockIterator.close();
				} else if (pick < 0.80) {
					// 10% of the time range(h, f)
					final int hash = rand.nextInt(500) + offset;
					final int from = rand.nextInt(500);
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> iterator = table.range(hash, from);
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> mockIterator = table.range(hash,
							from);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeHF;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
					iterator.close();
					mockIterator.close();
				} else {
					// 20% of the time range(h, f, t)
					final int hash = rand.nextInt(500) + offset;
					final int from = rand.nextInt(500);
					final int to = rand.nextInt(500);
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> iterator = table.range(hash, from,
							to);
					final TableIterator<RangeTableRow<Integer, Integer, Integer>> mockIterator = table.range(hash, from,
							to);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeHFT;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
					iterator.close();
					mockIterator.close();
				}
			}

			final long seconds = (System.currentTimeMillis() - start) / 1000;

			final StringBuffer out = new StringBuffer().append("[").append(Thread.currentThread().getName())
					.append("] table creations: ").append(tables / seconds).append("/s, deletes: ")
					.append(deletes / seconds).append("/s, writes: ").append(writes / seconds).append("/s, reads: ")
					.append(reads / seconds).append("/s, range(hash): ").append(rangeH / seconds)
					.append("/s, range(hash, from): ").append(rangeHF / seconds).append("/s, range(hash, from, to): ")
					.append(rangeHFT / seconds).append("/s, total reads: ")
					.append((reads + rangeH + rangeHF + rangeHFT) / seconds).append("/s");

			System.out.println(out.toString());
		}
	}
}
