package ezdb.serde;

import java.nio.ByteBuffer;

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

	default O fromBuffer(final ByteBuffer buffer) {
		final byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		buffer.flip();
		return fromBytes(bytes);
	}

	default void toBuffer(final ByteBuffer buffer, final O obj) {
		final byte[] bytes = toBytes(obj);
		buffer.put(bytes);
		buffer.flip();
	}

	public O fromBytes(byte[] bytes);

	public byte[] toBytes(O obj);

	@SuppressWarnings("unchecked")
	default <T> T unwrap(final Class<T> type) {
		if (type.isAssignableFrom(getClass())) {
			return (T) this;
		} else {
			return null;
		}
	}
}
