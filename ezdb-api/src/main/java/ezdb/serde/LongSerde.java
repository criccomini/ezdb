package ezdb.serde;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

public class LongSerde implements Serde<Long> {
	public static final LongSerde get = new LongSerde();
	private static final byte[] EMPTY_BYTES = new byte[0];

	@Override
	public Long fromBuffer(final ByteBuf buffer) {
		if (buffer == null || buffer.readableBytes() == 0) {
			return null;
		}
		return buffer.readLong();
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Long obj) {
		if (obj == null) {
			return;
		}
		buffer.writeLong(obj);
	}

	@Override
	public Long fromBuffer(final ByteBuffer buffer) {
		if (buffer == null || buffer.remaining() == 0) {
			return null;
		}
		return buffer.getLong(buffer.position());
	}

	@Override
	public void toBuffer(final ByteBuffer buffer, final Long obj) {
		if (obj == null) {
			return;
		}
		buffer.putLong(buffer.position(), obj);
	}

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
