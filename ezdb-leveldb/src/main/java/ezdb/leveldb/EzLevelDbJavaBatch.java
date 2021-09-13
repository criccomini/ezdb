package ezdb.leveldb;

import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLevelDbJavaBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final DB db;
	private final WriteBatch writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;

	public EzLevelDbJavaBatch(final DB db, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {
		this.db = db;
		this.writeBatch = db.createWriteBatch();
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
		db.write(writeBatch);
	}

	@Override
	public void close() throws IOException {
		writeBatch.close();
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		// writing operations need to work with byte arrays
		final byte[] valueBytes = valueSerde.toBytes(value);
		final byte[] keyBytes = Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		writeBatch.put(keyBytes, valueBytes);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		/*
		 * delete does not work when we try zero copy here, maybe because the delete is
		 * performed async?
		 */
		writeBatch.delete(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
	}

}
