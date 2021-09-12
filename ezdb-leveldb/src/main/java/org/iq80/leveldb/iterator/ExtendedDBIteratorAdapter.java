package org.iq80.leveldb.iterator;

import java.io.Closeable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.util.Slice;

public class ExtendedDBIteratorAdapter implements Closeable {

	private static final MethodHandle SEEKING_ITERATOR_FIELD_GETTER;

	static {
		try {
			final Field seekingIteratorField = DBIteratorAdapter.class.getDeclaredField("seekingIterator");
			seekingIteratorField.setAccessible(true);
			SEEKING_ITERATOR_FIELD_GETTER = MethodHandles.lookup().unreflectGetter(seekingIteratorField);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

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

	public static ExtendedDBIteratorAdapter wrap(final DBIterator iterator) {
		final DBIteratorAdapter cIterator = (DBIteratorAdapter) iterator;
		try {
			final SnapshotSeekingIterator seekingIterator = (SnapshotSeekingIterator) SEEKING_ITERATOR_FIELD_GETTER
					.invoke(cIterator);
			return new ExtendedDBIteratorAdapter(seekingIterator);
		} catch (final Throwable e) {
			throw new RuntimeException(e);
		}
	}

}
