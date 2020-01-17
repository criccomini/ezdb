package ezdb.treemap.object;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Comparator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import ezdb.Db;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.comparator.ComparableComparator;

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
public class TestEzObjectTreeMapDbJniTorture {
	public static final int NUM_THREADS = 10;
	public static final int ITERATIONS = 300000;
	public static final String tableName = "torture";

	public Db<Object> db;

	@Before
	public void before() {
		db = new EzObjectTreeMapDb();
	}

	@Test
	public void testTortureEzLevelDb() throws InterruptedException {
		db.deleteTable(tableName);
		Thread[] threads = new Thread[NUM_THREADS];

		for (int i = 0; i < NUM_THREADS; ++i) {
			threads[i] = new Thread(new TortureRunnable(i, db));
			threads[i].start();
		}

		for (int i = 0; i < NUM_THREADS; ++i) {
			threads[i].join();
		}
	}

	public static class TortureRunnable implements Runnable {
		private int offset;
		private Db<Object> db;

		public TortureRunnable(int threadId, Db<Object> db) {
			this.offset = threadId * 1000;
			this.db = db;
		}

		public void run() {
			Random rand = new Random();
			RangeTable<Integer, Integer, Integer> table = db.getTable(tableName, null, null, null);
			Comparator<Integer> integerComparator = ComparableComparator.get();
			RangeTable<Integer, Integer, Integer> mockTable = new ObjectTreeMapTable<Integer, Integer, Integer>(
					integerComparator, integerComparator);
			long tables = 0, deletes = 0, writes = 0, reads = 0, rangeH = 0, rangeHF = 0, rangeHFT = 0;
			long start = System.currentTimeMillis();

			for (int i = 0; i < ITERATIONS; ++i) {
				// pick something to do
				double pick = rand.nextDouble();
				int hashKey = rand.nextInt(500) + offset;
				int rangeKey = rand.nextInt(500);
				int value = rand.nextInt(500);

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
					int hash = rand.nextInt(500) + offset;
					TableIterator<Integer, Integer, Integer> iterator = table.range(hash);
					TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeH;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
				} else if (pick < 0.80) {
					// 10% of the time range(h, f)
					int hash = rand.nextInt(500) + offset;
					int from = rand.nextInt(500);
					TableIterator<Integer, Integer, Integer> iterator = table.range(hash, from);
					TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash, from);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeHF;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
				} else {
					// 20% of the time range(h, f, t)
					int hash = rand.nextInt(500) + offset;
					int from = rand.nextInt(500);
					int to = rand.nextInt(500);
					TableIterator<Integer, Integer, Integer> iterator = table.range(hash, from, to);
					TableIterator<Integer, Integer, Integer> mockIterator = table.range(hash, from, to);

					while (iterator.hasNext() && mockIterator.hasNext()) {
						assertEquals(mockIterator.next(), iterator.next());
						++rangeHFT;
					}

					assertFalse(iterator.hasNext());
					assertFalse(mockIterator.hasNext());
				}
			}

			long seconds = (System.currentTimeMillis() - start) / 1000;

			if (seconds > 0) {
				StringBuffer out = new StringBuffer().append("[").append(Thread.currentThread().getName())
						.append("] table creations: ").append(tables / seconds).append("/s, deletes: ")
						.append(deletes / seconds).append("/s, writes: ").append(writes / seconds).append("/s, reads: ")
						.append(reads / seconds).append("/s, range(hash): ").append(rangeH / seconds)
						.append("/s, range(hash, from): ").append(rangeHF / seconds)
						.append("/s, range(hash, from, to): ").append(rangeHFT / seconds).append("/s, total reads: ")
						.append((reads + rangeH + rangeHF + rangeHFT) / seconds).append("/s");

				System.out.println(out.toString());
			}
		}
	}
}
