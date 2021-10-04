package ezdb.lmdb.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.lmdbjava.Txn.NotReadyException;

import ezdb.serde.Serde;
import ezdb.table.Batch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class EzLmDbBatch<H, V> implements Batch<H, V> {

	private final Dbi<ByteBuffer> db;
	private final Txn<ByteBuffer> txn;
	private final Serde<H> hashKeySerde;
	private final Serde<V> valueSerde;
	private final ByteBuf keyBuffer;
	private final ByteBuf valueBuffer;

	public EzLmDbBatch(final Env<ByteBuffer> env, final Dbi<ByteBuffer> db, final Serde<H> hashKeySerde,
			final Serde<V> valueSerde) {
		this.db = db;
		this.txn = env.txnWrite();
		this.hashKeySerde = hashKeySerde;
		this.valueSerde = valueSerde;
		this.keyBuffer = ByteBufAllocator.DEFAULT.directBuffer();
		this.valueBuffer = ByteBufAllocator.DEFAULT.directBuffer();
	}

	@Override
	public void put(final H hashKey, final V value) {
		keyBuffer.clear();
		hashKeySerde.toBuffer(keyBuffer, hashKey);
		valueBuffer.clear();
		valueSerde.toBuffer(valueBuffer, value);
		db.put(txn, keyBuffer.nioBuffer(), valueBuffer.nioBuffer());
	}

	@Override
	public void delete(final H hashKey) {
		keyBuffer.clear();
		hashKeySerde.toBuffer(keyBuffer, hashKey);
		db.delete(txn, keyBuffer.nioBuffer());
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

}
