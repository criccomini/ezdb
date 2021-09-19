package ezdb.serde;

import java.nio.ByteBuffer;

import ezdb.util.Util;
import io.netty.buffer.ByteBuf;

public final class VoidSerde implements Serde<Void> {

	public static final VoidSerde get = new VoidSerde();

	private VoidSerde() {
	}

	@Override
	public Void fromBuffer(final ByteBuffer buffer) {
		return null;
	}

	@Override
	public void toBuffer(final ByteBuffer buffer, final Void obj) {
	}

	@Override
	public Void fromBuffer(final ByteBuf buffer) {
		return null;
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Void obj) {
	}

	@Override
	public Void fromBytes(final byte[] bytes) {
		return null;
	}

	@Override
	public byte[] toBytes(final Void obj) {
		return Util.EMPTY_BYTES;
	}

}
