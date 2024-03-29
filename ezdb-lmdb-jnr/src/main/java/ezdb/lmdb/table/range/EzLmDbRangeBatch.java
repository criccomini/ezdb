package ezdb.lmdb.table.range;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.lmdbjava.Txn.NotReadyException;

import ezdb.serde.Serde;
import ezdb.table.range.RangeBatch;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLmDbRangeBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final Dbi<ByteBuffer> db;
	private final Txn<ByteBuffer> txn;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private final ByteBuf keyBuffer;
	private final ByteBuf valueBuffer;

	public EzLmDbRangeBatch(final Env<ByteBuffer> env, final Dbi<ByteBuffer> db, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
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
		this.keyBuffer.release(keyBuffer.refCnt());
		this.valueBuffer.release(valueBuffer.refCnt());
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
		keyBuffer.clear();
		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		valueBuffer.clear();
		valueSerde.toBuffer(valueBuffer, value);
		db.put(txn, keyBuffer.nioBuffer(), valueBuffer.nioBuffer());
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
		keyBuffer.clear();
		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		db.delete(txn, keyBuffer.nioBuffer());
	}

}
