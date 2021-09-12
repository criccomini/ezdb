package ezdb.lmdb;

import java.io.IOException;
import java.nio.ByteBuffer;

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

	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> db;
	private final Txn<ByteBuffer> txn;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;

	public EzLmDbBatch(final Env<ByteBuffer> env, final Dbi<ByteBuffer> db, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		this.env = env;
		this.db = db;
		this.txn = env.txnWrite();
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
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		final ByteBuf keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		final ByteBuf valueBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		valueSerde.toBuffer(valueBuffer, value);
		try {
			db.put(txn, keyBuffer.nioBuffer(), valueBuffer.nioBuffer());
		} finally {
			keyBuffer.release(keyBuffer.refCnt());
			valueBuffer.release(valueBuffer.refCnt());
		}
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
		try {
			Util.combineBuf(buffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
			db.delete(txn, buffer.nioBuffer());
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

}
