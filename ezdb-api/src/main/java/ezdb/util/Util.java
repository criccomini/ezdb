package ezdb.util;

import java.nio.ByteBuffer;
import java.util.Comparator;
import ezdb.serde.Serde;

public class Util {

  public static <H, R> byte[] combine(Serde<H> hashKeySerde, Serde<R> rangeKeySerde, H hashKey, R rangeKey) {
    byte[] rangeBytes = new byte[0];

    if (rangeKey != null) {
      rangeBytes = rangeKeySerde.toBytes(rangeKey);
    }

    return combine(hashKeySerde.toBytes(hashKey), rangeBytes);
  }

  /**
   * Utility function to combine a hash key and range key. Hash/range key pairs
   * are expected to be persisted in the following byte format:
   * 
   * <pre>
   * [4 byte hash key length]
   * [arbitrary hash key bytes]
   * [4 byte range key length]
   * [arbitrary range key bytes]
   * </pre>
   * 
   * @param hashKeyBytes
   *          Are the hash key's bytes.
   * @param rangeKeyBytes
   *          Are the range key's bytes.
   * @return Returns a byte array defined by the format above.
   */
  public static byte[] combine(byte[] hashKeyBytes, byte[] rangeKeyBytes) {
    byte[] result = new byte[8 + hashKeyBytes.length + rangeKeyBytes.length];
    System.arraycopy(ByteBuffer.allocate(4).putInt(hashKeyBytes.length).array(), 0, result, 0, 4);
    System.arraycopy(hashKeyBytes, 0, result, 4, hashKeyBytes.length);
    System.arraycopy(ByteBuffer.allocate(4).putInt(rangeKeyBytes.length).array(), 0, result, 4 + hashKeyBytes.length, 4);
    System.arraycopy(rangeKeyBytes, 0, result, 8 + hashKeyBytes.length, rangeKeyBytes.length);
    return result;
  }

  public static int compareKeys(
      Comparator<byte[]> hashKeyComparator,
      Comparator<byte[]> rangeKeyComparator,
      byte[] k1,
      byte[] k2) {
    // First hash key
    ByteBuffer k1Buffer = ByteBuffer.wrap(k1);
    int k1HashKeyLength = k1Buffer.getInt();
    byte[] k1HashKeyBytes = new byte[k1HashKeyLength];
    k1Buffer.get(k1HashKeyBytes);

    // Second hash key
    ByteBuffer k2Buffer = ByteBuffer.wrap(k2);
    int k2HashKeyLength = k2Buffer.getInt();
    byte[] k2HashKeyBytes = new byte[k2HashKeyLength];
    k2Buffer.get(k2HashKeyBytes);

    int hashComparison = hashKeyComparator.compare(k1HashKeyBytes, k2HashKeyBytes);

    if (rangeKeyComparator != null && hashComparison == 0) {
      // First range key
      int k1RangeKeyLength = k1Buffer.getInt();
      byte[] k1RangeKeyBytes = new byte[k1RangeKeyLength];
      k1Buffer.get(k1RangeKeyBytes);

      // Second range key
      int k2RangeKeyLength = k2Buffer.getInt();
      byte[] k2RangeKeyBytes = new byte[k2RangeKeyLength];
      k2Buffer.get(k2RangeKeyBytes);

      return rangeKeyComparator.compare(k1RangeKeyBytes, k2RangeKeyBytes);
    }

    return hashComparison;
  }
}
