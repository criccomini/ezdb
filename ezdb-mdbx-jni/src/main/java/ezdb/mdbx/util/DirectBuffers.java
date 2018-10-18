package ezdb.mdbx.util;

import java.nio.ByteBuffer;

import org.lmdbjava.Env;

public final class DirectBuffers {

	private DirectBuffers() {
	}


	public static ByteBuffer wrap(byte[] bytes) {
		ByteBuffer directBuffer = ByteBuffer.allocateDirect(bytes.length);
		directBuffer.put(bytes);
		directBuffer.flip();
		return directBuffer;
	}

	public static byte[] array(ByteBuffer buffer) {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		buffer.rewind();
		return bytes;
	}

}
