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

	private final Env<ByteBuf> env;
	private final Dbi<ByteBuf> db;
	private final Txn<ByteBuf> txn;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final ByteBuf keyBuffer;
	private final ByteBuf valueBuffer;

	public EzLmDbBatch(final Env<ByteBuf> env, final Dbi<ByteBuf> db, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		this.env = env;
		this.db = db;
		this.txn = env.txnWrite();
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
		this.keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		this.valueBuffer = ByteBufAllocator.DEFAULT.directBuffer();
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
		flush();
		txn.close();
		keyBuffer.release(keyBuffer.refCnt());
		valueBuffer.release(valueBuffer.refCnt());
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		keyBuffer.clear();
		Util.combine(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		valueBuffer.clear();
		valueSerde.toBuffer(valueBuffer, value);
		db.put(txn, keyBuffer, valueBuffer);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		Util.combine(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		db.delete(txn, keyBuffer);
	}

}
