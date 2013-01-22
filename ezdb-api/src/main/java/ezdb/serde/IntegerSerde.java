package ezdb.serde;

import java.nio.ByteBuffer;

public class IntegerSerde implements Serde<Integer> {
  public static final IntegerSerde get = new IntegerSerde();

  @Override
  public Integer fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.put(bytes);
    buffer.flip();// need flip
    return buffer.getInt();
  }

  @Override
  public byte[] toBytes(Integer obj) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(obj);
    return buffer.array();
  }
}
