package ezleveldb.serde;

import java.io.UnsupportedEncodingException;

public class StringSerde implements Serde<String> {
  public static final StringSerde get = new StringSerde();

  @Override
  public String fromBytes(byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] toBytes(String obj) {
    try {
      return obj.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
