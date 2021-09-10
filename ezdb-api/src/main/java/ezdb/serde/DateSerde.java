package ezdb.serde;

import java.util.Date;

import io.netty.buffer.ByteBuf;

public class DateSerde implements Serde<Date> {

	public static final DateSerde get = new DateSerde();

	@Override
	public Date fromBuffer(final ByteBuf buffer) {
		final Long time = LongSerde.get.fromBuffer(buffer);
		if (time == null) {
			return null;
		} else {
			return new Date(time);
		}
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Date obj) {
		final Long time;
		if (obj == null) {
			time = null;
		} else {
			time = obj.getTime();
		}
		LongSerde.get.toBuffer(buffer, time);
	}

	@Override
	public Date fromBytes(final byte[] bytes) {
		final Long time = LongSerde.get.fromBytes(bytes);
		if (time == null) {
			return null;
		} else {
			return new Date(time);
		}
	}

	@Override
	public byte[] toBytes(final Date obj) {
		final Long time;
		if (obj == null) {
			time = null;
		} else {
			time = obj.getTime();
		}
		return LongSerde.get.toBytes(time);
	}

}