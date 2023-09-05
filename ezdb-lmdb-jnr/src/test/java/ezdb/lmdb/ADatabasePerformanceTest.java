package ezdb.lmdb;

import java.util.Objects;

public abstract class ADatabasePerformanceTest {
	
	static {
		TestInitializer.init();
	}

	protected static final int READS = 10;
	protected static final int VALUES = 1_000_000;
	protected static final String HASH_KEY = "HASH_KEY";
	protected static final int FLUSH_INTERVAL = 10_000;

	protected void printProgress(final String action, final long start, final double count, final double maxCount) {
		final long duration = System.currentTimeMillis() - start;
		System.out.println(action + ": " + count + "/" + maxCount + " (" + (count / maxCount * 100) + ") "
				+ (count / duration) + "/ms during " + duration);
	}

}
