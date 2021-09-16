package org.iq80.leveldb.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.iq80.leveldb.impl.DbConstants.L0_SLOWDOWN_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.L0_STOP_WRITES_TRIGGER;
import static org.iq80.leveldb.impl.DbConstants.NUM_NON_TABLE_CACHE_FILES;
import static org.iq80.leveldb.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.leveldb.impl.ValueType.DELETION;
import static org.iq80.leveldb.impl.ValueType.VALUE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_LONG;
import static org.iq80.leveldb.util.Slices.readLengthPrefixedBytes;
import static org.iq80.leveldb.util.Slices.writeLengthPrefixedBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.compression.Compressions;
import org.iq80.leveldb.compression.Compressor;
import org.iq80.leveldb.env.DbLock;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.env.File;
import org.iq80.leveldb.env.NoOpLogger;
import org.iq80.leveldb.env.SequentialFile;
import org.iq80.leveldb.env.WritableFile;
import org.iq80.leveldb.impl.Filename.FileInfo;
import org.iq80.leveldb.impl.Filename.FileType;
import org.iq80.leveldb.impl.WriteBatchImpl.Handler;
import org.iq80.leveldb.iterator.DBIteratorAdapter;
import org.iq80.leveldb.iterator.DbIterator;
import org.iq80.leveldb.iterator.InternalIterator;
import org.iq80.leveldb.iterator.MergingIterator;
import org.iq80.leveldb.iterator.SnapshotSeekingIterator;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.table.CustomUserComparator;
import org.iq80.leveldb.table.FilterPolicy;
import org.iq80.leveldb.table.TableBuilder;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.SafeListBuilder;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.util.Slices;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class ExtensibleDbImpl implements DB {
	protected final Options options;
	protected final boolean ownsLogger;
	protected final File databaseDir;
	protected final TableCache tableCache;
	protected final DbLock dbLock;
	protected final VersionSet versions;

	protected final AtomicBoolean shuttingDown = new AtomicBoolean();
	protected final ReentrantLock mutex = new ReentrantLock();
	protected final Condition backgroundCondition = mutex.newCondition();

	protected final List<Long> pendingOutputs = new ArrayList<>(); // todo
	protected final Deque<WriteBatchInternal> writers = new LinkedList<>();
	protected final SnapshotList snapshots = new SnapshotList(mutex);
	protected final WriteBatchImpl tmpBatch = new WriteBatchImpl();
	protected final Env env;
	protected final Compressor compressor;

	protected LogWriter log;

	protected MemTable memTable;
	protected volatile MemTable immutableMemTable;

	protected final InternalKeyComparator internalKeyComparator;

	protected volatile Throwable backgroundException;
	protected final ExecutorService compactionExecutor;
	protected Future<?> backgroundCompaction;

	protected ManualCompaction manualCompaction;

	protected final CompactionStats[] stats = new CompactionStats[DbConstants.NUM_LEVELS];

	public ExtensibleDbImpl(final Options rawOptions, final String dbname, final Env env) throws IOException {
		this.env = env;
		requireNonNull(rawOptions, "options is null");
		requireNonNull(dbname, "databaseDir is null");
		final File databaseDir = env.toFile(dbname);
		this.options = sanitizeOptions(databaseDir, rawOptions);
		this.ownsLogger = this.options.logger() != rawOptions.logger();

		this.compressor = Compressions.tryToGetCompressor(this.options.compressionType());
		if (compressor == null) {
			// Disable snappy if it's not available.
			this.options.compressionType(CompressionType.NONE);
		}

		this.databaseDir = databaseDir;

		if (this.options.filterPolicy() != null) {
			checkArgument(this.options.filterPolicy() instanceof FilterPolicy,
					"Filter policy must implement Java interface FilterPolicy");
			this.options.filterPolicy(InternalFilterPolicy.convert(this.options.filterPolicy()));
		}

		// use custom comparator if set
		internalKeyComparator = new InternalKeyComparator(newUserComparator());
		immutableMemTable = null;

		final ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
				.setNameFormat("leveldb-" + databaseDir.getName() + "-%s").setUncaughtExceptionHandler((t, e) -> {
					mutex.lock();
					try {
						if (backgroundException == null) {
							backgroundException = e;
						}
						options.logger().log("Unexpected exception occurred %s", e);
					} finally {
						mutex.unlock();
					}
				}).build();
		compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);

		// Reserve ten files or so for other uses and give the rest to TableCache.
		final int tableCacheSize = options.maxOpenFiles() - DbConstants.NUM_NON_TABLE_CACHE_FILES;
		tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator),
				options, env, Compressions.decompressor());

		// create the version set

		// create the database dir if it does not already exist
		databaseDir.mkdirs();
		checkArgument(databaseDir.exists(), "Database directory '%s' does not exist and could not be created",
				databaseDir);
		checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory", databaseDir);

		for (int i = 0; i < DbConstants.NUM_LEVELS; i++) {
			stats[i] = new CompactionStats();
		}

		mutex.lock();
		final Closer c = Closer.create();
		boolean success = false;
		try {
			// lock the database dir
			this.dbLock = env.tryLock(databaseDir.child(Filename.lockFileName()));
			c.register(dbLock::release);
			// <editor-fold desc="Recover">
			// verify the "current" file
			final File currentFile = databaseDir.child(Filename.currentFileName());
			if (!currentFile.canRead()) {
				checkArgument(options.createIfMissing(),
						"Database '%s' does not exist and the create if missing option is disabled", databaseDir);
				/** @see VersionSet#initializeIfNeeded() newDB() **/
			} else {
				checkArgument(!options.errorIfExists(),
						"Database '%s' exists and the error if exists option is enabled", databaseDir);
			}

			this.versions = new VersionSet(options, databaseDir, tableCache, internalKeyComparator, env);
			c.register(versions::release);
			// load (and recover) current version
			boolean saveManifest = versions.recover();

			// Recover from all newer log files than the ones named in the
			// descriptor (new log files may have been added by the previous
			// incarnation without registering them in the descriptor).
			//
			// Note that PrevLogNumber() is no longer used, but we pay
			// attention to it in case we are recovering a database
			// produced by an older version of leveldb.
			final long minLogNumber = versions.getLogNumber();
			final long previousLogNumber = versions.getPrevLogNumber();
			final Set<Long> expected = versions.getLiveFiles().stream().map(FileMetaData::getNumber)
					.collect(Collectors.toSet());
			final List<File> filenames = databaseDir.listFiles();

			final List<Long> logs = new ArrayList<>();
			for (final File filename : filenames) {
				final FileInfo fileInfo = Filename.parseFileName(filename);
				if (fileInfo != null) {
					expected.remove(fileInfo.getFileNumber());
					if (fileInfo.getFileType() == FileType.LOG && ((fileInfo.getFileNumber() >= minLogNumber)
							|| (fileInfo.getFileNumber() == previousLogNumber))) {
						logs.add(fileInfo.getFileNumber());
					}
				}
			}

			checkArgument(expected.isEmpty(), "%s missing files", expected.size());

			// Recover in the order in which the logs were generated
			final VersionEdit edit = new VersionEdit();
			Collections.sort(logs);
			for (final Iterator<Long> iterator = logs.iterator(); iterator.hasNext();) {
				final Long fileNumber = iterator.next();
				final RecoverResult result = recoverLogFile(fileNumber, !iterator.hasNext(), edit);
				saveManifest |= result.saveManifest;

				// The previous incarnation may not have written any MANIFEST
				// records after allocating this log number. So we manually
				// update the file number allocation counter in VersionSet.
				this.versions.markFileNumberUsed(fileNumber);

				if (versions.getLastSequence() < result.maxSequence) {
					versions.setLastSequence(result.maxSequence);
				}
			}
			// </editor-fold>

			// open transaction log
			if (memTable == null) {
				final long logFileNumber = versions.getNextFileNumber();
				this.log = Logs.createLogWriter(databaseDir.child(Filename.logFileName(logFileNumber)), logFileNumber,
						env);
				c.register(log);
				edit.setLogNumber(log.getFileNumber());
				memTable = new MemTable(internalKeyComparator);
			}

			if (saveManifest) {
				edit.setPreviousLogNumber(0);
				edit.setLogNumber(log.getFileNumber());
				// apply recovered edits
				versions.logAndApply(edit, mutex);
			}

			// cleanup unused files
			deleteObsoleteFiles();

			// schedule compactions
			maybeScheduleCompaction();
			success = true;
		} catch (final Throwable e) {
			throw c.rethrow(e);
		} finally {
			if (!success) {
				if (ownsLogger) { // only close logger if created internally
					c.register(this.options.logger());
				}
				c.close();
			}
			mutex.unlock();
		}
	}

	protected UserComparator newUserComparator() {
		final DBComparator comparator = options.comparator();
		final UserComparator userComparator;
		if (comparator != null) {
			return new CustomUserComparator(comparator);
		} else {
			return new BytewiseComparator();
		}
	}

	// Fix user-supplied options to be reasonable
	private static <T extends Comparable<T>> T clipToRange(final T in, final T min, final T max) {
		if (in.compareTo(min) < 0) {
			return min;
		}
		if (in.compareTo(max) > 0) {
			return max;
		}
		return in;
	}

	/**
	 * Ensure we do not use external values as is. Ensure value are in correct
	 * ranges and a copy of external Options instance is used.
	 */
	private Options sanitizeOptions(final File databaseDir, final Options src) throws IOException {
		final Options result = Options.fromOptions(src);
		result.maxOpenFiles(clipToRange(src.maxOpenFiles(), 64 + NUM_NON_TABLE_CACHE_FILES, 50000));
		result.writeBufferSize(clipToRange(src.writeBufferSize(), 64 << 10, 1 << 30));
		result.maxFileSize(clipToRange(src.maxFileSize(), 1 << 20, 1 << 30));
		result.blockSize(clipToRange(src.blockSize(), 1 << 10, 4 << 20));
		if (result.logger() == null && databaseDir != null && (databaseDir.isDirectory() || databaseDir.mkdirs())) {
			final File file = databaseDir.child(Filename.infoLogFileName());
			file.renameTo(databaseDir.child(Filename.oldInfoLogFileName()));
			result.logger(env.newLogger(file));
		}
		if (result.logger() == null) {
			result.logger(new NoOpLogger());
		}
		return result;
	}

	/**
	 * Wait for all background activity to finish and invalidate all cache. Only
	 * used to test that all file handles are closed correctly.
	 */
	@VisibleForTesting
	void invalidateAllCaches() {
		mutex.lock();
		try {
			while (backgroundCompaction != null && backgroundException == null) {
				backgroundCondition.awaitUninterruptibly();
			}
			tableCache.invalidateAll();
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public void close() {
		if (shuttingDown.getAndSet(true)) {
			return;
		}

		mutex.lock();
		try {
			while (backgroundCompaction != null) {
				backgroundCondition.awaitUninterruptibly();
			}
		} finally {
			mutex.unlock();
		}

		compactionExecutor.shutdown();
		try {
			compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		try {
			versions.release();
		} catch (final IOException ignored) {
		}
		try {
			log.close();
		} catch (final IOException ignored) {
		}
		tableCache.close();
		if (ownsLogger) {
			Closeables.closeQuietly(options.logger());
		}
		dbLock.release();
	}

	@Override
	public String getProperty(final String name) {
		if (!name.startsWith("leveldb.")) {
			return null;
		}
		final String key = name.substring("leveldb.".length());
		mutex.lock();
		try {
			Matcher matcher;
			matcher = Pattern.compile("num-files-at-level(\\d+)").matcher(key);
			if (matcher.matches()) {
				final int level = Integer.parseInt(matcher.group(1));
				return String.valueOf(versions.numberOfFilesInLevel(level));
			}
			matcher = Pattern.compile("stats").matcher(key);
			if (matcher.matches()) {
				final StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("                               Compactions\n");
				stringBuilder.append("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n");
				stringBuilder.append("--------------------------------------------------\n");
				for (int level = 0; level < DbConstants.NUM_LEVELS; level++) {
					final int files = versions.numberOfFilesInLevel(level);
					if (stats[level].micros > 0 || files > 0) {
						stringBuilder.append(String.format("%3d %8d %8.0f %9.0f %8.0f %9.0f%n", level, files,
								versions.numberOfBytesInLevel(level) / 1048576.0, stats[level].micros / 1e6,
								stats[level].bytesRead / 1048576.0, stats[level].bytesWritten / 1048576.0));
					}
				}
				return stringBuilder.toString();
			} else if ("sstables".equals(key)) {
				return versions.getCurrent().toString();
			} else if ("approximate-memory-usage".equals(key)) {
				long sizeTotal = tableCache.getApproximateMemoryUsage();
				if (memTable != null) {
					sizeTotal += memTable.approximateMemoryUsage();
				}
				if (immutableMemTable != null) {
					sizeTotal += immutableMemTable.approximateMemoryUsage();
				}
				return Long.toUnsignedString(sizeTotal);
			}
		} finally {
			mutex.unlock();
		}
		return null;
	}

	private void deleteObsoleteFiles() {
		checkState(mutex.isHeldByCurrentThread());
		if (backgroundException != null) {
			return;
		}
		// Make a set of all of the live files
		final List<Long> live = new ArrayList<>(this.pendingOutputs);
		for (final FileMetaData fileMetaData : versions.getLiveFiles()) {
			live.add(fileMetaData.getNumber());
		}

		final List<File> filesToDelete = new ArrayList<>();
		for (final File file : databaseDir.listFiles()) {
			final FileInfo fileInfo = Filename.parseFileName(file);
			if (fileInfo == null) {
				continue;
			}
			final long number = fileInfo.getFileNumber();
			boolean keep = true;
			switch (fileInfo.getFileType()) {
			case LOG:
				keep = ((number >= versions.getLogNumber()) || (number == versions.getPrevLogNumber()));
				break;
			case DESCRIPTOR:
				// Keep my manifest file, and any newer incarnations'
				// (in case there is a race that allows other incarnations)
				keep = (number >= versions.getManifestFileNumber());
				break;
			case TABLE:
				keep = live.contains(number);
				break;
			case TEMP:
				// Any temp files that are currently being written to must
				// be recorded in pending_outputs_, which is inserted into "live"
				keep = live.contains(number);
				break;
			case CURRENT:
			case DB_LOCK:
			case INFO_LOG:
				keep = true;
				break;
			}

			if (!keep) {
				if (fileInfo.getFileType() == FileType.TABLE) {
					tableCache.evict(number);
				}
				options.logger().log("Delete type=%s #%s", fileInfo.getFileType(), number);
				filesToDelete.add(file);
			}
		}
		// While deleting all files unblock other threads. All files being deleted
		// have unique names which will not collide with newly created files and
		// are therefore safe to delete while allowing other threads to proceed.
		mutex.unlock();
		try {
			filesToDelete.forEach(File::delete);
		} finally {
			mutex.lock();
		}
	}

	protected void maybeScheduleCompaction() {
		checkState(mutex.isHeldByCurrentThread());

		if (backgroundCompaction != null) {
			// Already scheduled
		} else if (shuttingDown.get()) {
			// DB is being shutdown; no more background compactions
		} else if (backgroundException != null) {
			// Already got an error; no more changes
		} else if (immutableMemTable == null && manualCompaction == null && !versions.needsCompaction()) {
			// No work to be done
		} else {
			backgroundCompaction = compactionExecutor.submit(this::backgroundCall);
		}
	}

	private void checkBackgroundException() {
		final Throwable e = backgroundException;
		if (e != null) {
			throw new BackgroundProcessingException(e);
		}
	}

	private void backgroundCall() {
		mutex.lock();
		try {
			checkState(backgroundCompaction != null, "Compaction was not correctly scheduled");

			try {
				if (!shuttingDown.get() && backgroundException == null) {
					backgroundCompaction();
				}
			} finally {
				backgroundCompaction = null;
			}
			// Previous compaction may have produced too many files in a level,
			// so reschedule another compaction if needed.
			maybeScheduleCompaction();
		} catch (final DatabaseShutdownException ignored) {
		} catch (final Throwable throwable) {
			recordBackgroundError(throwable);
		} finally {
			try {
				backgroundCondition.signalAll();
			} finally {
				mutex.unlock();
			}
		}
	}

	private void backgroundCompaction() throws IOException {
		checkState(mutex.isHeldByCurrentThread());

		if (immutableMemTable != null) {
			compactMemTable();
			return;
		}

		Compaction compaction;
		InternalKey manualEnd = null;
		final boolean isManual = manualCompaction != null;
		if (isManual) {
			final ManualCompaction m = this.manualCompaction;
			compaction = versions.compactRange(m.level, m.begin, m.end);
			m.done = compaction == null;
			if (compaction != null) {
				manualEnd = compaction.input(0, compaction.getLevelInputs().size() - 1).getLargest();
			}
			options.logger().log("Manual compaction at level-%s from %s .. %s; will stop at %s", m.level,
					(m.begin != null ? m.begin.toString() : "(begin)"), (m.end != null ? m.end.toString() : "(end)"),
					(m.done ? "(end)" : manualEnd));
		} else {
			compaction = versions.pickCompaction();
		}

		if (compaction == null) {
			// no compaction
		} else if (!isManual && compaction.isTrivialMove()) {
			// Move file to next level
			checkState(compaction.getLevelInputs().size() == 1);
			final FileMetaData fileMetaData = compaction.getLevelInputs().get(0);
			compaction.getEdit().deleteFile(compaction.getLevel(), fileMetaData.getNumber());
			compaction.getEdit().addFile(compaction.getLevel() + 1, fileMetaData);
			versions.logAndApply(compaction.getEdit(), mutex);
			options.logger().log("Moved #%s to level-%s %s bytes: %s", fileMetaData.getNumber(),
					compaction.getLevel() + 1, fileMetaData.getFileSize(), versions.levelSummary());
		} else {
			final CompactionState compactionState = new CompactionState(compaction);
			try {
				doCompactionWork(compactionState);
			} catch (final Exception e) {
				options.logger().log("Compaction error: %s", e.getMessage());
				recordBackgroundError(e);
			} finally {
				cleanupCompaction(compactionState);
				compaction.close(); // release resources
				deleteObsoleteFiles();
			}
		}
		if (compaction != null) {
			compaction.close();
		}

		// manual compaction complete
		if (isManual) {
			final ManualCompaction m = manualCompaction;
			if (backgroundException != null) {
				m.done = true;
			}
			if (!m.done) {
				m.begin = manualEnd;
			}
			manualCompaction = null;
		}
	}

	private void recordBackgroundError(final Throwable e) {
		checkState(mutex.isHeldByCurrentThread());
		final Throwable backgroundException = this.backgroundException;
		if (backgroundException == null) {
			this.backgroundException = e;
			backgroundCondition.signalAll();
		}
		Throwables.throwIfInstanceOf(e, Error.class);
	}

	private void cleanupCompaction(final CompactionState compactionState) throws IOException {
		checkState(mutex.isHeldByCurrentThread());

		if (compactionState.builder != null) {
			compactionState.builder.abandon();
			compactionState.builder = null;
		}
		if (compactionState.outfile != null) {
			// an error as occurred but we need to release the resources!
			compactionState.outfile.force();
			compactionState.outfile.close();
			compactionState.outfile = null;
		}

		for (final FileMetaData output : compactionState.outputs) {
			pendingOutputs.remove(output.getNumber());
		}
	}

	private static class RecoverResult {
		long maxSequence;
		boolean saveManifest;

		public RecoverResult(final long maxSequence, final boolean saveManifest) {
			this.maxSequence = maxSequence;
			this.saveManifest = saveManifest;
		}
	}

	private RecoverResult recoverLogFile(final long fileNumber, final boolean lastLog, final VersionEdit edit)
			throws IOException {
		checkState(mutex.isHeldByCurrentThread());
		final File file = databaseDir.child(Filename.logFileName(fileNumber));
		try (SequentialFile in = env.newSequentialFile(file)) {
			final LogMonitor logMonitor = LogMonitors.logMonitor(options.logger());

			// We intentionally make LogReader do checksumming even if
			// paranoidChecks==false so that corruptions cause entire commits
			// to be skipped instead of propagating bad information (like overly
			// large sequence numbers).
			final LogReader logReader = new LogReader(in, logMonitor, true, 0);

			options.logger().log("Recovering log #%s", fileNumber);

			// Read all the records and add to a memtable
			long maxSequence = 0;
			int compactions = 0;
			boolean saveManifest = false;
			MemTable mem = null;
			for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
				final SliceInput sliceInput = record.input();
				// read header
				if (sliceInput.available() < 12) {
					logMonitor.corruption(sliceInput.available(), "log record too small");
					continue;
				}
				final long sequenceBegin = sliceInput.readLong();
				final int updateSize = sliceInput.readInt();

				// read entries
				try (WriteBatchImpl writeBatch = readWriteBatch(sliceInput, updateSize)) {
					// apply entries to memTable
					if (mem == null) {
						mem = new MemTable(internalKeyComparator);
					}
					writeBatch.forEach(new InsertIntoHandler(mem, sequenceBegin));
				} catch (final Exception e) {
					if (!options.paranoidChecks()) {
						options.logger().log("Ignoring error %s", e);
					}
					Throwables.propagateIfPossible(e, IOException.class);
					throw new IOException(e);
				}

				// update the maxSequence
				final long lastSequence = sequenceBegin + updateSize - 1;
				if (lastSequence > maxSequence) {
					maxSequence = lastSequence;
				}

				// flush mem table if necessary
				if (mem.approximateMemoryUsage() > options.writeBufferSize()) {
					compactions++;
					saveManifest = true;
					writeLevel0Table(mem, edit, null);
					mem = null;
				}
			}

			// See if we should keep reusing the last log file.
			if (options.reuseLogs() && lastLog && compactions == 0) {
				Preconditions.checkState(this.log == null);
				Preconditions.checkState(this.memTable == null);
				final long originalSize = file.length();
				final WritableFile writableFile = env.newAppendableFile(file);
				options.logger().log("Reusing old log %s", file);
				this.log = Logs.createLogWriter(fileNumber, writableFile, originalSize);
				if (mem != null) {
					this.memTable = mem;
					mem = null;
				} else {
					// mem can be NULL if lognum exists but was empty.
					this.memTable = new MemTable(internalKeyComparator);
				}
			}

			// flush mem table
			if (mem != null && !mem.isEmpty()) {
				saveManifest = true;
				writeLevel0Table(mem, edit, null);
			}

			return new RecoverResult(maxSequence, saveManifest);
		}
	}

	@Override
	public byte[] get(final byte[] key) throws DBException {
		return get(key, new ReadOptions());
	}

	@Override
	public byte[] get(final byte[] key, final ReadOptions options) throws DBException {
		LookupKey lookupKey;
		LookupResult lookupResult;
		mutex.lock();
		try {
			final long lastSequence = options.snapshot() != null ? snapshots.getSequenceFrom(options.snapshot())
					: versions.getLastSequence();
			lookupKey = new LookupKey(Slices.wrappedBuffer(key), lastSequence);

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
				return value.getBytes();
			}
		}
		return null;
	}

	@Override
	public void put(final byte[] key, final byte[] value) throws DBException {
		put(key, value, new WriteOptions());
	}

	@Override
	public Snapshot put(final byte[] key, final byte[] value, final WriteOptions options) throws DBException {
		try (WriteBatchImpl writeBatch = new WriteBatchImpl()) {
			return writeInternal(writeBatch.put(key, value), options);
		}
	}

	@Override
	public void delete(final byte[] key) throws DBException {
		delete(key, new WriteOptions());
	}

	@Override
	public Snapshot delete(final byte[] key, final WriteOptions options) throws DBException {
		try (WriteBatchImpl writeBatch = new WriteBatchImpl()) {
			return writeInternal(writeBatch.delete(key), options);
		}
	}

	@Override
	public void write(final WriteBatch updates) throws DBException {
		writeInternal((WriteBatchImpl) updates, new WriteOptions());
	}

	@Override
	public Snapshot write(final WriteBatch updates, final WriteOptions options) throws DBException {
		return writeInternal((WriteBatchImpl) updates, options);
	}

	public Snapshot writeInternal(final WriteBatchImpl myBatch, final WriteOptions options) throws DBException {
		checkBackgroundException();
		final WriteBatchInternal w = new WriteBatchInternal(myBatch, options.sync(), mutex.newCondition());
		mutex.lock();
		try {
			writers.offerLast(w);
			while (!w.done && writers.peekFirst() != w) {
				w.backgroundCondition.awaitUninterruptibly();
			}
			if (w.done) {
				w.checkExceptions();
				return options.snapshot() ? snapshots.newSnapshot(versions.getLastSequence()) : null;
			}
			final ValueHolder<WriteBatchInternal> lastWriterVh = new ValueHolder<>(w);
			Throwable error = null;
			try {
				multipleWriteGroup(myBatch, options, lastWriterVh);
			} catch (final Exception e) {
				// all writers must be notified of this exception
				error = e;
			}

			final WriteBatchInternal lastWrite = lastWriterVh.getValue();
			while (true) {
				final WriteBatchInternal ready = writers.peekFirst();
				writers.pollFirst();
				if (ready != w) {
					ready.error = error;
					ready.done = true;
					ready.signal();
				}
				if (ready == lastWrite) {
					break;
				}
			}

			// Notify new head of write queue
			if (!writers.isEmpty()) {
				writers.peekFirst().signal();
			}
			checkBackgroundException();
			if (error != null) {
				Throwables.propagateIfPossible(error, DBException.class);
				throw new DBException(error);
			}
			return options.snapshot() ? snapshots.newSnapshot(versions.getLastSequence()) : null;
		} finally {
			mutex.unlock();
		}
	}

	private void multipleWriteGroup(final WriteBatchImpl myBatch, final WriteOptions options,
			final ValueHolder<WriteBatchInternal> lastWriter) {
		long sequenceEnd;
		WriteBatchImpl updates = null;
		// May temporarily unlock and wait.
		makeRoomForWrite(myBatch == null);
		if (myBatch != null) {
			updates = buildBatchGroup(lastWriter);

			// Get sequence numbers for this change set
			final long sequenceBegin = versions.getLastSequence() + 1;
			sequenceEnd = sequenceBegin + updates.size() - 1;

			// Add to log and apply to memtable. We can release the lock
			// during this phase since "w" is currently responsible for logging
			// and protects against concurrent loggers and concurrent writes
			// into mem_.
			// log and memtable are modified by makeRoomForWrite
			mutex.unlock();
			try {
				// Log write
				final Slice record = writeWriteBatch(updates, sequenceBegin);
				log.addRecord(record, options.sync());
				// Update memtable
				// this.memTable is modified by makeRoomForWrite
				updates.forEach(new InsertIntoHandler(this.memTable, sequenceBegin));
			} catch (final Exception e) {
				// The state of the log file is indeterminate: the log record we
				// just added may or may not show up when the DB is re-opened.
				// So we force the DB into a mode where all future writes fail.
				mutex.lock();
				try {
					// we need to be inside lock to record exception
					recordBackgroundError(e);
				} finally {
					mutex.unlock();
				}
			} finally {
				mutex.lock();
			}
			if (updates == tmpBatch) {
				tmpBatch.clear();
			}
			// Reserve this sequence in the version set
			versions.setLastSequence(sequenceEnd);
		}
	}

	/**
	 * REQUIRES: Writer list must be non-empty REQUIRES: First writer must have a
	 * non-NULL batch
	 */
	private WriteBatchImpl buildBatchGroup(final ValueHolder<WriteBatchInternal> lastWriter) {
		checkArgument(!writers.isEmpty(), "A least one writer is required");
		final WriteBatchInternal first = writers.peekFirst();
		WriteBatchImpl result = first.batch;
		checkArgument(result != null, "Batch must be non null");

		int sizeInit;
		sizeInit = first.batch.getApproximateSize();
		/*
		 * Allow the group to grow up to a maximum size, but if the original write is
		 * small, limit the growth so we do not slow down the small write too much.
		 */
		int maxSize = 1 << 20;
		if (sizeInit <= (128 << 10)) {
			maxSize = sizeInit + (128 << 10);
		}

		int size = 0;
		lastWriter.setValue(first);
		for (final WriteBatchInternal w : writers) {
			if (w.sync && !lastWriter.getValue().sync) {
				// Do not include a sync write into a batch handled by a non-sync write.
				break;
			}

			if (w.batch != null) {
				size += w.batch.getApproximateSize();
				if (size > maxSize) {
					// Do not make batch too big
					break;
				}

				// Append to result
				if (result == first.batch) {
					// Switch to temporary batch instead of disturbing caller's batch
					result = tmpBatch;
					checkState(result.size() == 0, "Temp batch should be clean");
					result.append(first.batch);
				} else if (first.batch != w.batch) {
					result.append(w.batch);
				}
			}
			lastWriter.setValue(w);
		}
		return result;
	}

	@Override
	public WriteBatchImpl createWriteBatch() {
		checkBackgroundException();
		return new WriteBatchImpl();
	}

	@Override
	public DBIteratorAdapter iterator() {
		return iterator(new ReadOptions());
	}

	@Override
	public DBIteratorAdapter iterator(final ReadOptions options) {
		mutex.lock();
		try {
			final InternalIterator rawIterator = internalIterator(options);

			// filter out any entries not visible in our snapshot
			final long snapshot = getSnapshot(options);
			final SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot,
					internalKeyComparator.getUserComparator(), new RecordBytesListener());
			return new DBIteratorAdapter(snapshotIterator);
		} finally {
			mutex.unlock();
		}
	}

	InternalIterator internalIterator(final ReadOptions options) {
		mutex.lock();
		try (SafeListBuilder<InternalIterator> builder = SafeListBuilder.builder()) {
			// merge together the memTable, immutableMemTable, and tables in version set
			builder.add(memTable.iterator());
			if (immutableMemTable != null) {
				builder.add(immutableMemTable.iterator());
			}
			final Version current = versions.getCurrent();
			builder.addAll(current.getLevelIterators(options));
			current.retain();
			return new DbIterator(new MergingIterator(builder.build(), internalKeyComparator), () -> {
				mutex.lock();
				try {
					current.release();
				} finally {
					mutex.unlock();
				}
			});
		} catch (final IOException e) {
			throw new DBException(e);
		} finally {
			mutex.unlock();
		}
	}

	/**
	 * Record a sample of bytes read at the specified internal key. Samples are
	 * taken approximately once every config::READ_BYTES_PERIOD bytes.
	 */
	void recordReadSample(final InternalKey key) {
		mutex.lock();
		try {
			if (versions.getCurrent().recordReadSample(key)) {
				maybeScheduleCompaction();
			}
		} finally {
			mutex.unlock();
		}
	}

	@Override
	public Snapshot getSnapshot() {
		checkBackgroundException();
		mutex.lock();
		try {
			return snapshots.newSnapshot(versions.getLastSequence());
		} finally {
			mutex.unlock();
		}
	}

	protected long getSnapshot(final ReadOptions options) {
		long snapshot;
		if (options.snapshot() != null) {
			snapshot = snapshots.getSequenceFrom(options.snapshot());
		} else {
			snapshot = versions.getLastSequence();
		}
		return snapshot;
	}

	private void makeRoomForWrite(boolean force) {
		checkState(mutex.isHeldByCurrentThread());
		checkState(!writers.isEmpty());

		boolean allowDelay = !force;

		while (true) {
			checkBackgroundException();
			if (allowDelay && versions.numberOfFilesInLevel(0) > L0_SLOWDOWN_WRITES_TRIGGER) {
				// We are getting close to hitting a hard limit on the number of
				// L0 files. Rather than delaying a single write by several
				// seconds when we hit the hard limit, start delaying each
				// individual write by 1ms to reduce latency variance. Also,
				// this delay hands over some CPU to the compaction thread in
				// case it is sharing the same core as the writer.
				try {
					mutex.unlock();
					Thread.sleep(1);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new DBException(e);
				} finally {
					mutex.lock();
				}

				// Do not delay a single write more than once
				allowDelay = false;
			} else if (!force && memTable.approximateMemoryUsage() <= options.writeBufferSize()) {
				// There is room in current memtable
				break;
			} else if (immutableMemTable != null) {
				// We have filled up the current memtable, but the previous
				// one is still being compacted, so we wait.
				options.logger().log("Current memtable full; waiting...");
				backgroundCondition.awaitUninterruptibly();
			} else if (versions.numberOfFilesInLevel(0) >= L0_STOP_WRITES_TRIGGER) {
				// There are too many level-0 files.
				options.logger().log("Too many L0 files; waiting...");
				backgroundCondition.awaitUninterruptibly();
			} else {
				// Attempt to switch to a new memtable and trigger compaction of old
				checkState(versions.getPrevLogNumber() == 0);

				// close the existing log
				try {
					log.close();
				} catch (final IOException e) {
					throw new DBException("Unable to close log file " + log, e);
				}

				// open a new log
				final long logNumber = versions.getNextFileNumber();
				try {
					this.log = Logs.createLogWriter(databaseDir.child(Filename.logFileName(logNumber)), logNumber, env);
				} catch (final IOException e) {
					throw new DBException("Unable to open new log file "
							+ databaseDir.child(Filename.logFileName(logNumber)).getPath(), e);
				}

				// create a new mem table
				immutableMemTable = memTable;
				memTable = new MemTable(internalKeyComparator);

				// Do not force another compaction there is space available
				force = false;

				maybeScheduleCompaction();
			}
		}
	}

	private void compactMemTable() throws IOException {
		checkState(mutex.isHeldByCurrentThread());
		checkState(immutableMemTable != null);

		try {
			// Save the contents of the memtable as a new Table
			final VersionEdit edit = new VersionEdit();
			final Version base = versions.getCurrent();
			base.retain();
			writeLevel0Table(immutableMemTable, edit, base);
			base.release();

			if (shuttingDown.get()) {
				throw new DatabaseShutdownException("Database shutdown during memtable compaction");
			}

			// Replace immutable memtable with the generated Table
			edit.setPreviousLogNumber(0);
			edit.setLogNumber(log.getFileNumber()); // Earlier logs no longer needed
			versions.logAndApply(edit, mutex);

			immutableMemTable = null;
			deleteObsoleteFiles();
		} finally {
			backgroundCondition.signalAll();
		}
	}

	private void writeLevel0Table(final MemTable mem, final VersionEdit edit, final Version base) throws IOException {
		final long startMicros = env.nowMicros();
		checkState(mutex.isHeldByCurrentThread());

		// skip empty mem table
		if (mem.isEmpty()) {
			return;
		}

		// write the memtable to a new sstable
		final long fileNumber = versions.getNextFileNumber();
		pendingOutputs.add(fileNumber);
		options.logger().log("Level-0 table #%s: started", fileNumber);

		mutex.unlock();
		FileMetaData meta;
		try {
			meta = buildTable(mem, fileNumber);
		} finally {
			mutex.lock();
		}
		options.logger().log("Level-0 table #%s: %s bytes", meta.getNumber(), meta.getFileSize());
		pendingOutputs.remove(fileNumber);

		// Note that if file size is zero, the file has been deleted and
		// should not be added to the manifest.
		int level = 0;
		if (meta.getFileSize() > 0) {
			final Slice minUserKey = meta.getSmallest().getUserKey();
			final Slice maxUserKey = meta.getLargest().getUserKey();
			if (base != null) {
				level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
			}
			edit.addFile(level, meta);
		}
		this.stats[level].add(env.nowMicros() - startMicros, 0, meta.getFileSize());
	}

	private FileMetaData buildTable(final MemTable data, final long fileNumber) throws IOException {
		final File file = databaseDir.child(Filename.tableFileName(fileNumber));
		try {
			InternalKey smallest = null;
			InternalKey largest = null;
			try (WritableFile writableFile = env.newWritableFile(file)) {
				final TableBuilder tableBuilder = new TableBuilder(options, writableFile,
						new InternalUserComparator(internalKeyComparator), compressor);

				try (InternalIterator it = data.iterator()) {
					for (boolean valid = it.seekToFirst(); valid; valid = it.next()) {
						// update keys
						final InternalKey key = it.key();
						if (smallest == null) {
							smallest = key;
						}
						largest = key;

						tableBuilder.add(key.encode(), it.value());
					}
				}

				tableBuilder.finish();
				writableFile.force();
			}

			if (smallest == null) {
				// empty iterator
				file.delete();
				return new FileMetaData(fileNumber, 0, null, null);
			}
			final FileMetaData fileMetaData = new FileMetaData(fileNumber, file.length(), smallest, largest);

			// verify table can be opened
			tableCache.newIterator(fileMetaData, new ReadOptions()).close();

			return fileMetaData;
		} catch (final IOException e) {
			file.delete();
			throw e;
		}
	}

	private void doCompactionWork(final CompactionState compactionState) throws IOException {
		final long startMicros = env.nowMicros();
		long immMicros = 0; // Micros spent doing imm_ compactions
		options.logger().log("Compacting %s@%s + %s@%s files", compactionState.compaction.input(0).size(),
				compactionState.compaction.getLevel(), compactionState.compaction.input(1).size(),
				compactionState.compaction.getLevel() + 1);

		checkState(mutex.isHeldByCurrentThread());
		checkArgument(versions.numberOfBytesInLevel(compactionState.getCompaction().getLevel()) > 0);
		checkArgument(compactionState.builder == null);
		checkArgument(compactionState.outfile == null);

		compactionState.smallestSnapshot = snapshots.isEmpty() ? versions.getLastSequence() : snapshots.getOldest();

		// Release mutex while we're actually doing the compaction work
		final MergingIterator mergingIterator = versions.makeInputIterator(compactionState.compaction);
		mutex.unlock();
		try (MergingIterator iterator = mergingIterator) {
			Slice currentUserKey = null;
			boolean hasCurrentUserKey = false;

			long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
			for (boolean valid = iterator.seekToFirst(); valid && !shuttingDown.get(); valid = iterator.next()) {
				// always give priority to compacting the current mem table
				if (immutableMemTable != null) {
					final long immStart = env.nowMicros();
					mutex.lock();
					try {
						compactMemTable();
					} finally {
						mutex.unlock();
					}
					immMicros += (env.nowMicros() - immStart);
				}
				final InternalKey key = iterator.key();
				if (compactionState.compaction.shouldStopBefore(key) && compactionState.builder != null) {
					finishCompactionOutputFile(compactionState);
				}

				// Handle key/value, add to state, etc.
				boolean drop = false;
				// todo if key doesn't parse (it is corrupted),
				if (false /* !ParseInternalKey(key, &ikey) */) {
					// do not hide error keys
					currentUserKey = null;
					hasCurrentUserKey = false;
					lastSequenceForKey = MAX_SEQUENCE_NUMBER;
				} else {
					if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(),
							currentUserKey) != 0) {
						// First occurrence of this user key
						currentUserKey = key.getUserKey();
						hasCurrentUserKey = true;
						lastSequenceForKey = MAX_SEQUENCE_NUMBER;
					}

					if (lastSequenceForKey <= compactionState.smallestSnapshot) {
						// Hidden by an newer entry for same user key
						drop = true; // (A)
					} else if (key.getValueType() == DELETION
							&& key.getSequenceNumber() <= compactionState.smallestSnapshot
							&& compactionState.compaction.isBaseLevelForKey(key.getUserKey())) {
						// For this user key:
						// (1) there is no data in higher levels
						// (2) data in lower levels will have larger sequence numbers
						// (3) data in layers that are being compacted here and have
						// smaller sequence numbers will be dropped in the next
						// few iterations of this loop (by rule (A) above).
						// Therefore this deletion marker is obsolete and can be dropped.
						drop = true;
					}

					lastSequenceForKey = key.getSequenceNumber();
				}

				if (!drop) {
					// Open output file if necessary
					if (compactionState.builder == null) {
						openCompactionOutputFile(compactionState);
					}
					if (compactionState.builder.getEntryCount() == 0) {
						compactionState.currentSmallest = key;
					}
					compactionState.currentLargest = key;
					compactionState.builder.add(key.encode(), iterator.value());

					// Close output file if it is big enough
					if (compactionState.builder.getFileSize() >= compactionState.compaction.getMaxOutputFileSize()) {
						finishCompactionOutputFile(compactionState);
					}
				}
			}

			if (shuttingDown.get()) {
				throw new DatabaseShutdownException("DB shutdown during compaction");
			}
			if (compactionState.builder != null) {
				finishCompactionOutputFile(compactionState);
			}
		} finally {
			final long micros = env.nowMicros() - startMicros - immMicros;
			long bytesRead = 0;
			for (int which = 0; which < 2; which++) {
				for (int i = 0; i < compactionState.compaction.input(which).size(); i++) {
					bytesRead += compactionState.compaction.input(which, i).getFileSize();
				}
			}
			long bytesWritten = 0;
			for (int i = 0; i < compactionState.outputs.size(); i++) {
				bytesWritten += compactionState.outputs.get(i).getFileSize();
			}
			mutex.lock();
			this.stats[compactionState.compaction.getLevel() + 1].add(micros, bytesRead, bytesWritten);
		}
		installCompactionResults(compactionState);
		options.logger().log("compacted to: %s", versions.levelSummary());
	}

	private void openCompactionOutputFile(final CompactionState compactionState) throws IOException {
		requireNonNull(compactionState, "compactionState is null");
		checkArgument(compactionState.builder == null, "compactionState builder is not null");

		long fileNumber;
		mutex.lock();
		try {
			fileNumber = versions.getNextFileNumber();
			pendingOutputs.add(fileNumber);
			compactionState.currentFileNumber = fileNumber;
			compactionState.currentFileSize = 0;
			compactionState.currentSmallest = null;
			compactionState.currentLargest = null;
		} finally {
			mutex.unlock();
		}
		final File file = databaseDir.child(Filename.tableFileName(fileNumber));
		compactionState.outfile = env.newWritableFile(file);
		compactionState.builder = new TableBuilder(options, compactionState.outfile,
				new InternalUserComparator(internalKeyComparator), compressor);
	}

	private void finishCompactionOutputFile(final CompactionState compactionState) throws IOException {
		requireNonNull(compactionState, "compactionState is null");
		checkArgument(compactionState.outfile != null);
		checkArgument(compactionState.builder != null);

		final long outputNumber = compactionState.currentFileNumber;
		checkArgument(outputNumber != 0);

		final long currentEntries = compactionState.builder.getEntryCount();
		compactionState.builder.finish();

		final long currentBytes = compactionState.builder.getFileSize();
		compactionState.currentFileSize = currentBytes;
		compactionState.totalBytes += currentBytes;

		final FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
				compactionState.currentFileSize, compactionState.currentSmallest, compactionState.currentLargest);
		compactionState.outputs.add(currentFileMetaData);

		compactionState.builder = null;

		compactionState.outfile.force();
		compactionState.outfile.close();
		compactionState.outfile = null;

		if (currentEntries > 0) {
			// Verify that the table is usable
			tableCache.newIterator(outputNumber, new ReadOptions()).close();
			options.logger().log("Generated table #%s@%s: %s keys, %s bytes", outputNumber,
					compactionState.compaction.getLevel(), currentEntries, currentBytes);
		}
	}

	private void installCompactionResults(final CompactionState compact) throws IOException {
		checkState(mutex.isHeldByCurrentThread());
		options.logger().log("Compacted %s@%s + %s@%s files => %s bytes", compact.compaction.input(0).size(),
				compact.compaction.getLevel(), compact.compaction.input(1).size(), compact.compaction.getLevel() + 1,
				compact.totalBytes);

		// Add compaction outputs
		compact.compaction.addInputDeletions(compact.compaction.getEdit());
		final int level = compact.compaction.getLevel();
		for (final FileMetaData output : compact.outputs) {
			compact.compaction.getEdit().addFile(level + 1, output);
			pendingOutputs.remove(output.getNumber());
		}

		versions.logAndApply(compact.compaction.getEdit(), mutex);
	}

	@VisibleForTesting
	int numberOfFilesInLevel(final int level) {
		mutex.lock();
		Version v;
		try {
			v = versions.getCurrent();
		} finally {
			mutex.unlock();
		}
		return v.numberOfFilesInLevel(level);
	}

	@Override
	public long[] getApproximateSizes(final Range... ranges) {
		requireNonNull(ranges, "ranges is null");
		final long[] sizes = new long[ranges.length];
		for (int i = 0; i < ranges.length; i++) {
			final Range range = ranges[i];
			sizes[i] = getApproximateSizes(range);
		}
		return sizes;
	}

	public long getApproximateSizes(final Range range) {
		mutex.lock();
		Version v;
		try {
			v = versions.getCurrent();
			v.retain();
			try {
				final InternalKey startKey = new InternalKey(Slices.wrappedBuffer(range.start()), MAX_SEQUENCE_NUMBER,
						VALUE);
				final InternalKey limitKey = new InternalKey(Slices.wrappedBuffer(range.limit()), MAX_SEQUENCE_NUMBER,
						VALUE);
				final long startOffset = v.getApproximateOffsetOf(startKey);
				final long limitOffset = v.getApproximateOffsetOf(limitKey);
				return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
			} finally {
				v.release();
			}
		} finally {
			mutex.unlock();
		}
	}

	public long getMaxNextLevelOverlappingBytes() {
		mutex.lock();
		try {
			return versions.getMaxNextLevelOverlappingBytes();
		} finally {
			mutex.unlock();
		}
	}

	private static class CompactionState {
		private final Compaction compaction;

		private final List<FileMetaData> outputs = new ArrayList<>();

		private long smallestSnapshot;

		// State kept for output being generated
		private WritableFile outfile;
		private TableBuilder builder;

		// Current file being generated
		private long currentFileNumber;
		private long currentFileSize;
		private InternalKey currentSmallest;
		private InternalKey currentLargest;

		private long totalBytes;

		private CompactionState(final Compaction compaction) {
			this.compaction = compaction;
		}

		public Compaction getCompaction() {
			return compaction;
		}
	}

	private static class ManualCompaction {
		private final int level;
		private InternalKey begin;
		private final InternalKey end;
		private boolean done;

		private ManualCompaction(final int level, final InternalKey begin, final InternalKey end) {
			this.level = level;
			this.begin = begin;
			this.end = end;
		}
	}

	// Per level compaction stats. stats[level] stores the stats for
	// compactions that produced data for the specified "level".
	private static class CompactionStats {
		long micros;
		long bytesRead;
		long bytesWritten;

		CompactionStats() {
			this.micros = 0;
			this.bytesRead = 0;
			this.bytesWritten = 0;
		}

		public void add(final long micros, final long bytesRead, final long bytesWritten) {
			this.micros += micros;
			this.bytesRead += bytesRead;
			this.bytesWritten += bytesWritten;
		}
	}

	private WriteBatchImpl readWriteBatch(final SliceInput record, final int updateSize) throws IOException {
		final WriteBatchImpl writeBatch = new WriteBatchImpl();
		int entries = 0;
		while (record.isReadable()) {
			entries++;
			final ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
			if (valueType == VALUE) {
				final Slice key = readLengthPrefixedBytes(record);
				final Slice value = readLengthPrefixedBytes(record);
				writeBatch.put(key, value);
			} else if (valueType == DELETION) {
				final Slice key = readLengthPrefixedBytes(record);
				writeBatch.delete(key);
			} else {
				throw new IllegalStateException("Unexpected value type " + valueType);
			}
		}

		if (entries != updateSize) {
			throw new IOException(
					String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
		}

		return writeBatch;
	}

	static Slice writeWriteBatch(final WriteBatchImpl updates, final long sequenceBegin) {
		final Slice record = Slices.allocate(SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize());
		final SliceOutput sliceOutput = record.output();
		sliceOutput.writeLong(sequenceBegin);
		sliceOutput.writeInt(updates.size());
		updates.forEach(new Handler() {
			@Override
			public void put(final Slice key, final Slice value) {
				sliceOutput.writeByte(VALUE.getPersistentId());
				writeLengthPrefixedBytes(sliceOutput, key);
				writeLengthPrefixedBytes(sliceOutput, value);
			}

			@Override
			public void delete(final Slice key) {
				sliceOutput.writeByte(DELETION.getPersistentId());
				writeLengthPrefixedBytes(sliceOutput, key);
			}
		});
		return record.slice(0, sliceOutput.size());
	}

	public static class DatabaseShutdownException extends DBException {
		public DatabaseShutdownException() {
		}

		public DatabaseShutdownException(final String message) {
			super(message);
		}
	}

	public static class BackgroundProcessingException extends DBException {
		public BackgroundProcessingException(final Throwable cause) {
			super(cause);
		}
	}

	private final Object suspensionMutex = new Object();
	private int suspensionCounter;

	@Override
	public void suspendCompactions() throws InterruptedException {
		compactionExecutor.execute(() -> {
			try {
				synchronized (suspensionMutex) {
					suspensionCounter++;
					suspensionMutex.notifyAll();
					while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
						suspensionMutex.wait(500);
					}
				}
			} catch (final InterruptedException e) {
			}
		});
		synchronized (suspensionMutex) {
			while (suspensionCounter < 1) {
				suspensionMutex.wait();
			}
		}
	}

	@Override
	public void resumeCompactions() {
		synchronized (suspensionMutex) {
			suspensionCounter--;
			suspensionMutex.notifyAll();
		}
	}

	@Override
	public void compactRange(final byte[] begin, final byte[] end) throws DBException {
		final Slice smallestUserKey = begin == null ? null : new Slice(begin, 0, begin.length);
		final Slice largestUserKey = end == null ? null : new Slice(end, 0, end.length);
		int maxLevelWithFiles = 1;
		mutex.lock();
		try {
			final Version base = versions.getCurrent();
			for (int level = 1; level < DbConstants.NUM_LEVELS; level++) {
				if (base.overlapInLevel(level, smallestUserKey, largestUserKey)) {
					maxLevelWithFiles = level;
				}
			}
		} finally {
			mutex.unlock();
		}
		testCompactMemTable(); // TODO: Skip if memtable does not overlap
		for (int level = 0; level < maxLevelWithFiles; level++) {
			testCompactRange(level, smallestUserKey, largestUserKey);
		}
	}

	@VisibleForTesting
	void testCompactRange(final int level, final Slice begin, final Slice end) throws DBException {
		checkArgument(level >= 0);
		checkArgument(level + 1 < DbConstants.NUM_LEVELS);

		final InternalKey beginStorage = begin == null ? null
				: new InternalKey(begin, SequenceNumber.MAX_SEQUENCE_NUMBER, VALUE);
		final InternalKey endStorage = end == null ? null : new InternalKey(end, 0, DELETION);
		final ManualCompaction manual = new ManualCompaction(level, beginStorage, endStorage);
		mutex.lock();
		try {
			while (!manual.done && !shuttingDown.get() && backgroundException == null) {
				if (manualCompaction == null) { // Idle
					manualCompaction = manual;
					maybeScheduleCompaction();
				} else { // Running either my compaction or another compaction.
					backgroundCondition.awaitUninterruptibly();
				}
			}
			if (manualCompaction == manual) {
				// Cancel my manual compaction since we aborted early for some reason.
				manualCompaction = null;
			}
		} finally {
			mutex.unlock();
		}
	}

	@VisibleForTesting
	void testCompactMemTable() throws DBException {
		// NULL batch means just wait for earlier writes to be done
		writeInternal(null, new WriteOptions());
		// Wait until the compaction completes
		mutex.lock();

		try {
			while (immutableMemTable != null && backgroundException == null) {
				backgroundCondition.awaitUninterruptibly();
			}
			if (immutableMemTable != null) {
				if (backgroundException != null) {
					throw new DBException(backgroundException);
				}
			}
		} finally {
			mutex.unlock();
		}
	}

	/**
	 * Wait for all background activity to finish; only usable in controlled
	 * environment.
	 */
	@VisibleForTesting
	void waitForBackgroundCompactationToFinish() {
		mutex.lock();
		try {
			while (backgroundCompaction != null && !shuttingDown.get() && backgroundException == null) {
				backgroundCondition.awaitUninterruptibly();
			}
		} finally {
			mutex.unlock();
		}
	}

	public static boolean destroyDB(final File dbname, final Env env) throws IOException {
		// Ignore error in case directory does not exist
		if (!dbname.exists()) {
			return true;
		}
		final List<File> filenames = dbname.listFiles();

		boolean res = true;
		final File lockFile = dbname.child(Filename.lockFileName());
		final DbLock lock = env.tryLock(lockFile);
		try {
			for (final File filename : filenames) {
				final FileInfo fileInfo = Filename.parseFileName(filename);
				if (fileInfo != null && fileInfo.getFileType() != FileType.DB_LOCK) { // Lock file will be deleted at
																						// end
					res &= filename.delete();
				}
			}
		} finally {
			try {
				lock.release(); // Ignore error since state is already gone
			} catch (final Exception ignore) {
			}
		}
		lockFile.delete();
		dbname.delete(); // Ignore error in case dir contains other files
		return res;
	}

	public class RecordBytesListener implements SnapshotSeekingIterator.IRecordBytesListener {
		private final Random r;
		private int bytesReadUntilSampling;

		RecordBytesListener() {
			this.r = new Random();
			this.bytesReadUntilSampling = getRandomCompactionPeriod(r);
		}

		@Override
		public void record(final InternalKey internalKey, final int bytes) {
			bytesReadUntilSampling -= bytes;
			while (bytesReadUntilSampling < 0) {
				bytesReadUntilSampling += getRandomCompactionPeriod(r);
				ExtensibleDbImpl.this.recordReadSample(internalKey);
			}
		}

		/**
		 * Picks the number of bytes that can be read until a compaction is scheduled.
		 *
		 * @param r
		 */
		private int getRandomCompactionPeriod(final Random r) {
			return r.nextInt(2 * DbConstants.READ_BYTES_PERIOD);
		}
	}

	private class WriteBatchInternal {
		private final WriteBatchImpl batch;
		private final boolean sync;
		private final Condition backgroundCondition;
		boolean done = false;
		public Throwable error;

		WriteBatchInternal(final WriteBatchImpl batch, final boolean sync, final Condition backgroundCondition) {
			this.batch = batch;
			this.sync = sync;
			this.backgroundCondition = backgroundCondition;
		}

		void signal() {
			backgroundCondition.signal();
		}

		void checkExceptions() {
			checkBackgroundException();
			if (error instanceof Error) {
				throw (Error) error;
			}
			if (error != null) {
				throw new DBException(error);
			}
		}
	}

	@Override
	public String toString() {
		return this.getClass().getName() + "{" + databaseDir + "}";
	}

}
