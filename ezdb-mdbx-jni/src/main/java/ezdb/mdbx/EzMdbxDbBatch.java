package ezdb.mdbx;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import ezdb.DbException;
import ezdb.batch.RangeBatch;
import ezdb.mdbx.util.DirectBuffers;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzMdbxDbBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Txn<ByteBuffer> txn;
	private Serde<H> hashKeySerde;
	private Serde<R> rangeKeySerde;
	private Serde<V> valueSerde;

	public EzMdbxDbBatch(Env<ByteBuffer> env, Dbi<ByteBuffer> db, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
			Serde<V> valueSerde) {
		this.env = env;
		this.db = db;
		this.txn = env.txnWrite();
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
		txn.commit();
	}

	@Override
	public void close() throws IOException {
		flush();
		txn.close();
	}

	@Override
	public void put(H hashKey, R rangeKey, V value) {
		db.put(txn, DirectBuffers.wrap(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)),
				DirectBuffers.wrap(valueSerde.toBytes(value)));
	}

	@Override
	public void delete(H hashKey, R rangeKey) {
		db.delete(txn, DirectBuffers.wrap(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey)));
	}

}
