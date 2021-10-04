package ezdb.leveldb.table.range;

import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteBatch;

import ezdb.serde.Serde;
import ezdb.table.range.RangeBatch;
import ezdb.util.Util;

public class EzLevelDbJniRangeBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final DB db;
	private final WriteBatch writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;

	public EzLevelDbJniRangeBatch(final DB db, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
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
		final byte[] valueBytes = valueSerde.toBytes(value);
		final byte[] keyBytes = Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		writeBatch.put(keyBytes, valueBytes);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		writeBatch.delete(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
	}

}
