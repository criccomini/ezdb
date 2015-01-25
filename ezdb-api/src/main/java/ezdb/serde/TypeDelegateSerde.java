package ezdb.serde;

import java.io.Serializable;
import java.sql.Date;
import java.util.Calendar;

public final class TypeDelegateSerde<O> implements Serde<O> {

	private final Serde<O> delegate;

	@SuppressWarnings("unchecked")
	public TypeDelegateSerde(final Class<O> type) {
		delegate = (Serde<O>) newDelegate(type);
	}

	protected Serde<?> newDelegate(final Class<O> type) {
		if (Byte.class.isAssignableFrom(type)
				|| byte.class.isAssignableFrom(type)) {
			return ByteSerde.get;
		} else if (Date.class.isAssignableFrom(type)) {
			return DateSerde.get;
		} else if (Long.class.isAssignableFrom(type)
				|| long.class.isAssignableFrom(type)) {
			return LongSerde.get;
		} else if (Integer.class.isAssignableFrom(type)
				|| int.class.isAssignableFrom(type)) {
			return IntegerSerde.get;
		} else if (Calendar.class.isAssignableFrom(type)) {
			return CalendarSerde.get;
		} else if (String.class.isAssignableFrom(type)) {
			return StringSerde.get;
		} else if (Void.class.isAssignableFrom(type)
				|| void.class.isAssignableFrom(type)) {
			return VoidSerde.get;
		} else if (type.isAssignableFrom(Serializable.class)) {
			// fallback to slower serialization
			return SerializingSerde.get();
		} else {
			// and give up if nothing found
			throw new IllegalArgumentException("No " + Serde.class
					+ " delegate available for type: " + type.getName());
		}
	}

	@Override
	public O fromBytes(final byte[] bytes) {
		return delegate.fromBytes(bytes);
	}

	@Override
	public byte[] toBytes(final O obj) {
		return delegate.toBytes(obj);
	}

}
