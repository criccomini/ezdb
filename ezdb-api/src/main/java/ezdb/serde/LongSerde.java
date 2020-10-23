package ezdb.serde;

import java.nio.ByteBuffer;

public class LongSerde implements Serde<Long> {
	public static final LongSerde get = new LongSerde();
	private static final byte[] EMPTY_BYTES = new byte[0];

	@Override
	public Long fromBytes(final byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return null;
		}
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getLong();
	}

	@Override
	public byte[] toBytes(final Long obj) {
		if (obj == null) {
			return EMPTY_BYTES;
		}
		final ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(obj);
		return buffer.array();
	}
}
