package ezdb.serde;

import java.nio.ByteBuffer;

public class LongSerde implements Serde<Long> {
  public static final LongSerde get = new LongSerde();

  @Override
  public Long fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes);
    buffer.flip();// need flip
    return buffer.getLong();
  }

  @Override
  public byte[] toBytes(Long obj) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(obj);
    return buffer.array();
  }
}
