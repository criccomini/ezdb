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

	public EzRocksDbBatch(RocksDB db, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde) {
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
			db.write(new WriteOptions(), writeBatch);
		} catch (RocksDBException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void close() throws IOException {
		writeBatch.dispose();;
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		writeBatch.put(
				Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey),
				valueSerde.toBytes(value));
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		writeBatch.remove(Util.combine(hashKeySerde, rangeKeySerde, hashKey,
				rangeKey));
	}

}
