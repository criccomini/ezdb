package ezdb.lmdb;

import java.io.IOException;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.lmdbjava.Txn.NotReadyException;

import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLmDbBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final Dbi<ByteBuf> db;
	private Txn<ByteBuf> txn;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private ByteBuf keyBuf;
	private ByteBuf valueBuf;

	public EzLmDbBatch(final ByteBufAllocator allocator, final Env<ByteBuf> env, final Dbi<ByteBuf> db,
			final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		this.db = db;
		this.txn = env.txnWrite();
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.keyBuf = allocator.buffer();
		this.valueBuf = allocator.buffer();
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
		try {
			txn.commit();
		} catch (final NotReadyException e) {
			// ignore
		}
	}

	@Override
	public void close() throws IOException {
		if (keyBuf != null) {
			flush();
			txn.close();
			keyBuf.release(keyBuf.refCnt());
			valueBuf.release(valueBuf.refCnt());
			keyBuf = null;
			valueBuf = null;
			txn = null;
		}
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		keyBuf.clear();
		valueBuf.clear();
		Util.combine(keyBuf, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		valueSerde.toBuffer(valueBuf, value);
		db.put(txn, keyBuf, valueBuf);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		keyBuf.clear();
		Util.combine(keyBuf, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		db.delete(txn, keyBuf);
	}

}
