package ezdb.serde;

import java.nio.ByteBuffer;

import ezdb.serde.VersionedSerde.Versioned;
import ezdb.util.Util;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * A wrapper serde that handles pre-pending some version number to a byte
 * payload. The format for the version byte array is:
 * 
 * <pre>
 * [8 byte version]
 * [arbitrary object bytes]
 * </pre>
 * 
 * @author criccomini
 * 
 * @param <O> The type of object that we wish to version.
 */
public class VersionedSerde<O> implements Serde<Versioned<O>> {

	private final Serde<O> objectSerde;

	public VersionedSerde(final Serde<O> objectSerde) {
		this.objectSerde = objectSerde;
	}

	@Override
	public Versioned<O> fromBuffer(final ByteBuf buffer) {
		final long version = LongSerde.get.fromBuffer(buffer);
		final O object = objectSerde.fromBuffer(buffer);
		return new Versioned<O>(object, version);
	}

	@Override
	public void toBuffer(final ByteBuf buffer, final Versioned<O> versioned) {
		LongSerde.get.toBuffer(buffer, versioned.getVersion());
		objectSerde.toBuffer(buffer, versioned.getObj());
	}

	@Override
	public Versioned<O> fromBuffer(final ByteBuffer buffer) {
		final int positionBefore = buffer.position();
		final long version = LongSerde.get.fromBuffer(buffer);
		Util.position(buffer, positionBefore + Long.BYTES);
		final O object = objectSerde.fromBuffer(buffer);
		Util.position(buffer, positionBefore);
		return new Versioned<O>(object, version);
	}

	@Override
	public void toBuffer(final ByteBuffer buffer, final Versioned<O> versioned) {
		final int positionBefore = buffer.position();
		LongSerde.get.toBuffer(buffer, versioned.getVersion());
		Util.position(buffer, positionBefore + Long.BYTES);
		objectSerde.toBuffer(buffer, versioned.getObj());
		Util.position(buffer, positionBefore);
	}

	@Override
	public byte[] toBytes(final Versioned<O> obj) {
		final ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer();
		try {
			LongSerde.get.toBuffer(buffer, obj.getVersion());
			objectSerde.toBuffer(buffer, obj.getObj());
			final byte[] bytes = new byte[buffer.readableBytes()];
			buffer.readBytes(bytes);
			return bytes;
		} finally {
			buffer.release(buffer.refCnt());
		}
	}

	@Override
	public Versioned<O> fromBytes(final byte[] bytes) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return fromBuffer(buffer);
	}

	public static class Versioned<O> {
		private final O obj;
		private final long version;

		public Versioned(final O obj, final long version) {
			this.obj = obj;
			this.version = version;
		}

		public O getObj() {
			return obj;
		}

		public long getVersion() {
			return version;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((obj == null) ? 0 : obj.hashCode());
			result = prime * result + (int) (version ^ (version >>> 32));
			return result;
		}

		@SuppressWarnings("rawtypes")
		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final Versioned other = (Versioned) obj;
			if (this.obj == null) {
				if (other.obj != null) {
					return false;
				}
			} else if (!this.obj.equals(other.obj)) {
				return false;
			}
			if (version != other.version) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Versioned [obj=" + obj + ", version=" + version + "]";
		}
	}
}
