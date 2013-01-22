package ezdb.serde;

public class ByteSerde implements Serde<byte[]> {
  public static final ByteSerde get = new ByteSerde();

  @Override
  public byte[] fromBytes(byte[] bytes) {
    return bytes;
  }

  @Override
  public byte[] toBytes(byte[] obj) {
    return obj;
  }
}
