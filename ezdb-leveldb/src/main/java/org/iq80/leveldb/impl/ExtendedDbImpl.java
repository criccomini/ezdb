package org.iq80.leveldb.impl;

import java.io.IOException;

import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.iterator.ExtendedDBIteratorAdapter;
import org.iq80.leveldb.iterator.InternalIterator;
import org.iq80.leveldb.iterator.SnapshotSeekingIterator;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.ExtendedCustomUserComparator;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Slice;

import ezdb.leveldb.util.ZeroCopyDBComparator;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class ExtendedDbImpl extends ExtensibleDbImpl {

	public ExtendedDbImpl(final Options rawOptions, final String dbname, final Env env) throws IOException {
		super(rawOptions, dbname, env);

	}

	@Override
	protected UserComparator newUserComparator() {
		final ZeroCopyDBComparator comparator = (ZeroCopyDBComparator) options.comparator();
		if (comparator != null) {
			return new ExtendedCustomUserComparator(comparator);
		} else {
			return new BytewiseComparator();
		}
	}

	public Slice get(final Slice key, final ReadOptions options) throws DBException {
		LookupKey lookupKey;
		LookupResult lookupResult;
		mutex.lock();
		try {
			final long lastSequence = options.snapshot() != null ? snapshots.getSequenceFrom(options.snapshot())
					: versions.getLastSequence();
			lookupKey = new LookupKey(key, lastSequence);

			// First look in the memtable, then in the immutable memtable (if any).
			final MemTable memTable = this.memTable;
			final MemTable immutableMemTable = this.immutableMemTable;
			final Version current = versions.getCurrent();
			current.retain();
			ReadStats readStats = null;
			mutex.unlock();
			try {
				lookupResult = memTable.get(lookupKey);
				if (lookupResult == null && immutableMemTable != null) {
					lookupResult = immutableMemTable.get(lookupKey);
				}

				if (lookupResult == null) {
					// Not in memTables; try live files in level order
					readStats = new ReadStats();
					lookupResult = current.get(options, lookupKey, readStats);
				}

				// schedule compaction if necessary
			} finally {
				mutex.lock();
				if (readStats != null && current.updateStats(readStats)) {
					maybeScheduleCompaction();
				}
				current.release();
			}
		} finally {
			mutex.unlock();
		}

		if (lookupResult != null) {
			final Slice value = lookupResult.getValue();
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	public Snapshot put(final Slice key, final Slice value, final WriteOptions options) throws DBException {
		try (WriteBatchImpl writeBatch = new WriteBatchImpl()) {
			return writeInternal(writeBatch.put(key, value), options);
		}
	}

	public Snapshot delete(final Slice key, final WriteOptions options) throws DBException {
		try (WriteBatchImpl writeBatch = new WriteBatchImpl()) {
			return writeInternal(writeBatch.delete(key), options);
		}
	}

	public ExtendedDBIteratorAdapter extendedIterator(final ReadOptions options) {
		mutex.lock();
		try {
			final InternalIterator rawIterator = internalIterator(options);

			// filter out any entries not visible in our snapshot
			final long snapshot = getSnapshot(options);
			final SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot,
					internalKeyComparator.getUserComparator(), new RecordBytesListener());
			return new ExtendedDBIteratorAdapter(snapshotIterator);
		} finally {
			mutex.unlock();
		}
	}
}
