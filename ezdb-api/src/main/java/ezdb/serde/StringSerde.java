package ezdb.serde;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import ezdb.DbException;
import io.netty.buffer.ByteBuf;

public class StringSerde implements Serde<String> {
	public static final StringSerde get = new StringSerde();

	@Override
	public String fromBuffer(final ByteBuf buffer) {
		try {
			final byte[] bytes = ByteSerde.get.fromBuffer(buffer);
			return new String(bytes, "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final String obj) {
		try {
			final byte[] bytes = obj.getBytes("UTF-8");
			ByteSerde.get.toBuffer(buffer, bytes);
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}

	@Override
	public String fromBuffer(final ByteBuffer buffer) {
		try {
			final byte[] bytes = ByteSerde.get.fromBuffer(buffer);
			return new String(bytes, "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}

	@Override
	public void toBuffer(final ByteBuffer buffer, final String obj) {
		try {
			final byte[] bytes = obj.getBytes("UTF-8");
			ByteSerde.get.toBuffer(buffer, bytes);
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}

	@Override
	public String fromBytes(final byte[] bytes) {
		try {
			return new String(bytes, "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}

	@Override
	public byte[] toBytes(final String obj) {
		try {
			return obj.getBytes("UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new DbException(e);
		}
	}
}
