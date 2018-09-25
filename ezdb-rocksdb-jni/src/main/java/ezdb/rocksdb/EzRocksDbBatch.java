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
	private Serde<H> hashKeySerde;
	private Serde<R> rangeKeySerde;
	private Serde<V> valueSerde;
	private WriteOptions writeOptions;

	public EzRocksDbBatch(RocksDB db, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde) {
		this.writeOptions = new WriteOptions();
		this.db = db;
		this.writeBatch = new WriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void put(H hashKey, V value) {
		put(hashKey, null, value);
	}

	@Override
	public void delete(H hashKey) {
		delete(hashKey, null);
	}

	@Override
	public void flush() {
		try {
			db.write(writeOptions, writeBatch);
		} catch (RocksDBException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void close() throws IOException {
		writeBatch.close();
		writeOptions.close();
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		try {
			writeBatch.put(
					Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey),
					valueSerde.toBytes(value));
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		try {
			writeBatch.delete(Util.combine(hashKeySerde, rangeKeySerde, hashKey,
					rangeKey));
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

}
