package ezdb.leveldb;

import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLevelDbBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final DB db;
	private final WriteBatch writeBatch;
	private Serde<H> hashKeySerde;
	private Serde<R> rangeKeySerde;
	private Serde<V> valueSerde;

	public EzLevelDbBatch(DB db, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde) {
		this.db = db;
		this.writeBatch = db.createWriteBatch();
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
		db.write(writeBatch);
	}

	@Override
	public void close() throws IOException {
		writeBatch.close();
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		writeBatch.put(
				Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey),
				valueSerde.toBytes(value));
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		writeBatch.delete(Util.combine(hashKeySerde, rangeKeySerde, hashKey,
				rangeKey));
	}

}
