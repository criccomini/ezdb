package ezdb.leveldb;

import java.io.IOException;

import org.iq80.leveldb.impl.ExtendedDbImpl;
import org.iq80.leveldb.impl.WriteBatchImpl;

import ezdb.batch.RangeBatch;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLevelDbJavaBatch<H, R, V> implements RangeBatch<H, R, V> {

	private final ExtendedDbImpl db;
	private final WriteBatchImpl writeBatch;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
//	private final ByteBuf keyBuffer;
//	private final ByteBuf valueBuffer;

	public EzLevelDbJavaBatch(final ExtendedDbImpl db, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {
		this.db = db;
		this.writeBatch = db.createWriteBatch();
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
//		this.keyBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
//		this.valueBuffer = ByteBufAllocator.DEFAULT.heapBuffer();
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
//		this.keyBuffer.release(keyBuffer.refCnt());
//		this.valueBuffer.release(valueBuffer.refCnt());
	}

	@Override
	public void put(final H hashKey, final R rangeKey, final V value) {
//		keyBuffer.clear();
//		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
//		valueBuffer.clear();
//		valueSerde.toBuffer(valueBuffer, value);
//		writeBatch.put(Slices.wrapCopy(keyBuffer), Slices.wrapCopy(valueBuffer));
		// writing operations need to work with byte arrays
		final byte[] valueBytes = valueSerde.toBytes(value);
		final byte[] keyBytes = Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey);
		writeBatch.put(keyBytes, valueBytes);
	}

	@Override
	public void delete(final H hashKey, final R rangeKey) {
//		keyBuffer.clear();
//		Util.combineBuf(keyBuffer, hashKeySerde, rangeKeySerde, hashKey, rangeKey);
//		writeBatch.delete(Slices.wrapCopy(keyBuffer));
		/*
		 * delete does not work when we try zero copy here, maybe because the delete is
		 * performed async?
		 */
		writeBatch.delete(Util.combineBytes(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
	}

}
