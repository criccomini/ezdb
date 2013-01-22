package ezdb.leveldb;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.iq80.leveldb.DBComparator;

/**
 * LevelDb provides a comparator interface that we can use to handle hash/range
 * pairs.
 * 
 * @author criccomini
 * 
 */
public class EzLevelDbComparator implements DBComparator {
  public static final String name = EzLevelDbComparator.class.toString();

  private final Comparator<byte[]> hashKeyComparator;
  private final Comparator<byte[]> rangeKeyComparator;

  public EzLevelDbComparator(Comparator<byte[]> hashKeyComparator, Comparator<byte[]> rangeKeyComparator) {
    this.hashKeyComparator = hashKeyComparator;
    this.rangeKeyComparator = rangeKeyComparator;
  }

  @Override
  public int compare(byte[] k1, byte[] k2) {
    return compareKeys(k1, k2, true);
  }

  @Override
  public byte[] findShortSuccessor(byte[] key) {
    return key;
  }

  @Override
  public byte[] findShortestSeparator(byte[] start, byte[] limit) {
    return null;
  }

  @Override
  public String name() {
    return name;
  }

  public int compareKeys(byte[] k1, byte[] k2, boolean includeRangeKey) {
    // First hash key
    ByteBuffer k1Buffer = ByteBuffer.wrap(k1);
    int k1PartitionKeyLength = k1Buffer.getInt();
    byte[] k1PartitionKeyBytes = new byte[k1PartitionKeyLength];
    k1Buffer.get(k1PartitionKeyBytes);

    // Second hash key
    ByteBuffer k2Buffer = ByteBuffer.wrap(k2);
    int k2PartitionKeyLength = k2Buffer.getInt();
    byte[] k2PartitionKeyBytes = new byte[k2PartitionKeyLength];
    k2Buffer.get(k2PartitionKeyBytes);

    int partitionComparison = this.hashKeyComparator.compare(k1PartitionKeyBytes, k2PartitionKeyBytes);

    if (includeRangeKey && partitionComparison == 0) {
      // First range key
      int k1OrderKeyLength = k1Buffer.getInt();
      byte[] k1OrderKeyBytes = new byte[k1OrderKeyLength];
      k1Buffer.get(k1OrderKeyBytes);

      // Second range key
      int k2OrderKeyLength = k2Buffer.getInt();
      byte[] k2OrderKeyBytes = new byte[k2OrderKeyLength];
      k2Buffer.get(k2OrderKeyBytes);

      return this.rangeKeyComparator.compare(k1OrderKeyBytes, k2OrderKeyBytes);
    }

    return partitionComparison;
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
}
