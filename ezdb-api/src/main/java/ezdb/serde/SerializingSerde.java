package ezdb.serde;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import ezdb.DbException;

public class SerializingSerde<E> implements Serde<E> {

	@SuppressWarnings("rawtypes")
	private static final SerializingSerde INSTANCE = new SerializingSerde();

	@SuppressWarnings("unchecked")
	public static <T> SerializingSerde<T> get() {
		return INSTANCE;
	}

	@SuppressWarnings("unchecked")
	@Override
	public E fromBytes(final byte[] bytes) {
		return (E) deserialize(bytes);
	}

	@Override
	public byte[] toBytes(final E obj) {
		return serialize((Serializable) obj);
	}

	private static void serialize(final Serializable obj, final OutputStream outputStream) {
		if (outputStream == null) {
			throw new IllegalArgumentException("The OutputStream must not be null");
		}
		ObjectOutputStream out = null;
		try {
			// stream closed in the finally
			out = new ObjectOutputStream(outputStream);
			out.writeObject(obj);

		} catch (final IOException ex) {
			throw new DbException(ex);
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (final IOException ex) {
				// ignore close exception
			}
		}
	}

	private static byte[] serialize(final Serializable obj) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
		serialize(obj, baos);
		return baos.toByteArray();
	}

	private static Object deserialize(final InputStream inputStream) {
		if (inputStream == null) {
			throw new IllegalArgumentException("The InputStream must not be null");
		}
		ObjectInputStream in = null;
		try {
			// stream closed in the finally
			in = new ObjectInputStream(inputStream);
			return in.readObject();

		} catch (final ClassNotFoundException ex) {
			throw new DbException(ex);
		} catch (final IOException ex) {
			throw new DbException(ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (final IOException ex) {
				// ignore close exception
			}
		}
	}

	private static Object deserialize(final byte[] objectData) {
		if (objectData == null) {
			throw new IllegalArgumentException("The byte[] must not be null");
		}
		final ByteArrayInputStream bais = new ByteArrayInputStream(objectData);
		return deserialize(bais);
	}

}