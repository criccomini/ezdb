package ezdb.rocksdb.table;

import java.io.IOException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import ezdb.DbException;
import ezdb.serde.Serde;
import ezdb.table.Batch;

public class EzRocksDbBatch<H, V> implements Batch<H, V> {

	private final RocksDB db;
	private final WriteBatch writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final WriteOptions writeOptions;

	public EzRocksDbBatch(final RocksDB db, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		this.writeOptions = new WriteOptions();
		this.db = db;
		this.writeBatch = new WriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void put(final H hashKey, final V value) {
		try {
			writeBatch.put(hashKeySerde.toBytes(hashKey), valueSerde.toBytes(value));
		} catch (final RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(final H hashKey) {
		try {
			writeBatch.delete(hashKeySerde.toBytes(hashKey));
		} catch (final RocksDBException e) {
			throw new RuntimeException(e);
		}
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

}
