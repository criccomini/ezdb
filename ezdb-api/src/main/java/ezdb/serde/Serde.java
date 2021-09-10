package ezdb.serde;

import io.netty.buffer.ByteBuf;

/**
 * A generic serializer that can be used to convert Java objects back and forth
 * to byte arrays.
 * 
 * @author criccomini
 * 
 * @param <O> The type of the object that a serde can convert.
 */
public interface Serde<O> {
	default O fromBuffer(final ByteBuf buffer) {
		final byte[] bytes = new byte[buffer.readableBytes()];
		buffer.readBytes(bytes);
		return fromBytes(bytes);
	}

	default void toBuffer(final ByteBuf buffer, final O obj) {
		final byte[] bytes = toBytes(obj);
		buffer.writeBytes(bytes);
	}

	public O fromBytes(byte[] bytes);

	public byte[] toBytes(O obj);
}
