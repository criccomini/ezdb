package ezdb.leveldb.table;

import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import ezdb.serde.Serde;
import ezdb.table.Batch;

public class EzLevelDbJniBatch<H, V> implements Batch<H, V> {

	private final DB db;
	private final WriteBatch writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;

	public EzLevelDbJniBatch(final DB db, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		this.db = db;
		this.writeBatch = db.createWriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void put(final H hashKey, final V value) {
		final byte[] valueBytes = valueSerde.toBytes(value);
		final byte[] keyBytes = hashKeySerde.toBytes(hashKey);
		writeBatch.put(keyBytes, valueBytes);
	}

	@Override
	public void delete(final H hashKey) {
		writeBatch.delete(hashKeySerde.toBytes(hashKey));
	}

	@Override
	public void flush() {
		db.write(writeBatch);
	}

	@Override
	public void close() throws IOException {
		writeBatch.close();
	}

}
