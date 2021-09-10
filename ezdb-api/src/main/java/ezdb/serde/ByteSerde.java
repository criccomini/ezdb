package ezdb.serde;

import io.netty.buffer.ByteBuf;

public class ByteSerde implements Serde<byte[]> {
	public static final ByteSerde get = new ByteSerde();

	@Override
	public byte[] fromBuffer(final ByteBuf buffer) {
		final byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		return bytes;
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final byte[] obj) {
		buffer.writeBytes(obj);
	}

	@Override
	public byte[] fromBytes(final byte[] bytes) {
		return bytes;
	}

	@Override
	public byte[] toBytes(final byte[] obj) {
		return obj;
	}
}
