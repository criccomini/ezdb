package ezdb.lmdb;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.lmdbjava.Txn.NotReadyException;

import ezdb.DbException;
import ezdb.batch.RangeBatch;
import ezdb.lmdb.util.DirectBuffers;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLmDbBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Txn<ByteBuffer> txn;
	private Serde<H> hashKeySerde;
	private Serde<R> rangeKeySerde;
	private Serde<V> valueSerde;

	public EzLmDbBatch(Env<ByteBuffer> env, Dbi<ByteBuffer> db, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
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
		try {
			txn.commit();
		} catch (NotReadyException e) {
			// ignore
		}
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
