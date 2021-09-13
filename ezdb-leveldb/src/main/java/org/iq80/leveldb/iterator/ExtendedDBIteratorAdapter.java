package org.iq80.leveldb.iterator;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.util.Slice;

public class ExtendedDBIteratorAdapter implements Closeable {

	private static final String ILLEGAL_STATE = "Illegal use of iterator after release";
	private final SnapshotSeekingIterator seekingIterator;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private Direction direction = Direction.FORWARD;

	public ExtendedDBIteratorAdapter(final SnapshotSeekingIterator seekingIterator) {
		this.seekingIterator = seekingIterator;
	}

	public boolean seekToFirst() {
		if (direction == Direction.RELEASED) {
			throw new DBException(ILLEGAL_STATE);
		}
		direction = Direction.FORWARD;
		return seekingIterator.seekToFirst();
	}

	public Slice getValue() {
		return seekingIterator.value();
	}

	public Slice getKey() {
		return seekingIterator.key();
	}

	public boolean seek(final Slice targetKey) {
		if (direction == Direction.RELEASED) {
			throw new DBException(ILLEGAL_STATE);
		}
		direction = Direction.FORWARD;
		return seekingIterator.seek(targetKey);
	}

	public boolean next() {
		if (direction == Direction.RELEASED) {
			throw new DBException(ILLEGAL_STATE);
		}
		if (direction != Direction.FORWARD) {
			direction = Direction.FORWARD;
		}
		return seekingIterator.next();
	}

	@Override
	public void close() {
		// This is an end user API.. he might screw up and close multiple times.
		// but we don't want the close multiple times as reference counts go bad.
		if (closed.compareAndSet(false, true)) {
			direction = Direction.RELEASED;
			seekingIterator.close();
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	public boolean seekToLast() {
		if (direction == Direction.RELEASED) {
			throw new DBException(ILLEGAL_STATE);
		}
		direction = Direction.REVERSE;
		return seekingIterator.seekToLast();
	}

	public boolean prev() {
		if (direction == Direction.RELEASED) {
			throw new DBException(ILLEGAL_STATE);
		}
		if (direction != Direction.REVERSE) {
			direction = Direction.REVERSE;
		}
		return seekingIterator.prev();
	}

}
