package ezdb.rocksdb;

import java.io.IOException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import ezdb.DbException;
import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzRocksDbBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final RocksDB db;
	private final WriteBatch writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final WriteOptions writeOptions;

	public EzRocksDbBatch(final RocksDB db, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {
		this.writeOptions = new WriteOptions();
		this.db = db;
		this.writeBatch = new WriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void put(final H hashKey, final V value) {
		put(hashKey, null, value);
	}

	@Override
	public void delete(final H hashKey) {
		delete(hashKey, null);
	}

	@Override
	public void flush() {
		try {
			db.write(writeOptions, writeBatch);
		} catch (final RocksDBException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void close() throws IOException {
		writeBatch.close();
		writeOptions.close();
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		try {
			writeBatch.put(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
		} catch (final RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		try {
			writeBatch.delete(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
		} catch (final RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

}
