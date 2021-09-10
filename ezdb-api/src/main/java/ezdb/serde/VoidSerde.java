package ezdb.serde;

import io.netty.buffer.ByteBuf;

public final class VoidSerde implements Serde<Void> {

	public static final VoidSerde get = new VoidSerde();

	private VoidSerde() {
	}

	@Override
	public Void fromBuffer(final ByteBuf buffer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Void obj) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Void fromBytes(final byte[] bytes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] toBytes(final Void obj) {
		throw new UnsupportedOperationException();
	}

}
