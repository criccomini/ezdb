package ezdb.serde;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

public class IntegerSerde implements Serde<Integer> {
	public static final IntegerSerde get = new IntegerSerde();

	@Override
	public Integer fromBuffer(final ByteBuf buffer) {
		return buffer.readInt();
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Integer obj) {
		buffer.writeInt(obj);
	}

	@Override
	public Integer fromBytes(final byte[] bytes) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return buffer.getInt();
	}

	@Override
	public byte[] toBytes(final Integer obj) {
		final ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(obj);
		return buffer.array();
	}
}
