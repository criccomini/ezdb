package ezdb.leveldb.table;

import java.io.IOException;

import org.iq80.leveldb.impl.ExtendedDbImpl;
import org.iq80.leveldb.impl.WriteBatchImpl;

import ezdb.serde.Serde;
import ezdb.table.Batch;
import ezdb.util.Util;

public class EzLevelDbJavaBatch<H, V> implements Batch<H, V> {

	private final ExtendedDbImpl db;
	private final WriteBatchImpl writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;

	public EzLevelDbJavaBatch(final ExtendedDbImpl db, final Serde<H> hashKeySerde, final Serde<V> valueSerde) {
		this.db = db;
		this.writeBatch = db.createWriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void put(final H hashKey, final V value) {
		final byte[] valueBytes = valueSerde.toBytes(value);
		final byte[] keyBytes = Util.combineBytes(hashKeySerde, hashKey);
		writeBatch.put(keyBytes, valueBytes);
	}

	@Override
	public void delete(final H hashKey) {
		writeBatch.delete(Util.combineBytes(hashKeySerde, hashKey));
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
