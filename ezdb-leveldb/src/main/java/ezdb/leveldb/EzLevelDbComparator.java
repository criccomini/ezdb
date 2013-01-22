package ezdb.leveldb;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.iq80.leveldb.DBComparator;

public class EzLevelDbComparator implements DBComparator {
  public static final String name = EzLevelDbComparator.class.toString();

  private final Comparator<byte[]> rangeKeyComparator;

  public EzLevelDbComparator(Comparator<byte[]> rangeKeyComparator) {
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

    ByteBuffer k1PartitionKey = ByteBuffer.wrap(k1PartitionKeyBytes);
    ByteBuffer k2PartitionKey = ByteBuffer.wrap(k2PartitionKeyBytes);

    int partitionComparison = k1PartitionKey.compareTo(k2PartitionKey);

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

  public static class LexicographicalComparator implements Comparator<byte[]> {
    public static final LexicographicalComparator get = new LexicographicalComparator();

    @Override
    public int compare(byte[] rangeKey1, byte[] rangeKey2) {
      return ByteBuffer.wrap(rangeKey1).compareTo(ByteBuffer.wrap(rangeKey2));
    }
  }
}
