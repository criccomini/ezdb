package org.iq80.leveldb.table;

import static java.util.Objects.requireNonNull;

import org.iq80.leveldb.util.Slice;

import ezdb.leveldb.EzLevelDbJavaComparator;

public class ExtendedCustomUserComparator implements UserComparator {
	private final EzLevelDbJavaComparator comparator;

	public ExtendedCustomUserComparator(final EzLevelDbJavaComparator comparator) {
		requireNonNull(comparator.name(), "User Comparator name can't be null");
		this.comparator = comparator;
	}

	@Override
	public String name() {
		return comparator.name();
	}

	@Override
	public Slice findShortestSeparator(final Slice start, final Slice limit) {
		final byte[] shortestSeparator = comparator.findShortestSeparator(start.getBytes(), limit.getBytes());
		requireNonNull(shortestSeparator, "User comparator returned null from findShortestSeparator()");
		return new Slice(shortestSeparator);
	}

	@Override
	public Slice findShortSuccessor(final Slice key) {
		final byte[] shortSuccessor = comparator.findShortSuccessor(key.getBytes());
		requireNonNull(comparator, "User comparator returned null from findShortSuccessor()");
		return new Slice(shortSuccessor);
	}

	@Override
	public int compare(final Slice o1, final Slice o2) {
		return comparator.compare(o1, o2);
	}
}
