package ezdb.serde;

import java.nio.ByteBuffer;

public class LongSerde implements Serde<Long> {
	public static final LongSerde get = new LongSerde();

	@Override
	public Long fromBytes(final byte[] bytes) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getLong();
	}

	@Override
	public byte[] toBytes(final Long obj) {
		final ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(obj);
		return buffer.array();
	}
}
