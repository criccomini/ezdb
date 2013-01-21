package ezleveldb;

import java.nio.ByteBuffer;
import org.iq80.leveldb.DBComparator;

public class PartitionOrderComparator implements DBComparator {
  public static final String name = PartitionOrderComparator.class.toString();

  @Override
  public int compare(byte[] k1, byte[] k2) {
    // First key
    ByteBuffer k1Buffer = ByteBuffer.wrap(k1);
    int k1PartitionKeyLength = k1Buffer.getInt();
    byte[] k1PartitionKeyBytes = new byte[k1PartitionKeyLength];
    k1Buffer.get(k1PartitionKeyBytes);
    int k1OrderKeyLength = k1Buffer.getInt();
    byte[] k1OrderKeyBytes = new byte[k1OrderKeyLength];
    k1Buffer.get(k1OrderKeyBytes);

    // Second key
    ByteBuffer k2Buffer = ByteBuffer.wrap(k2);
    int k2PartitionKeyLength = k2Buffer.getInt();
    byte[] k2PartitionKeyBytes = new byte[k2PartitionKeyLength];
    k2Buffer.get(k2PartitionKeyBytes);
    int k2OrderKeyLength = k2Buffer.getInt();
    byte[] k2OrderKeyBytes = new byte[k2OrderKeyLength];
    k2Buffer.get(k2OrderKeyBytes);

    ByteBuffer k1PartitionKey = ByteBuffer.wrap(k1PartitionKeyBytes);
    ByteBuffer k1OrderKey = ByteBuffer.wrap(k1OrderKeyBytes);
    ByteBuffer k2PartitionKey = ByteBuffer.wrap(k2PartitionKeyBytes);
    ByteBuffer k2OrderKey = ByteBuffer.wrap(k2OrderKeyBytes);

    int partitionComparison = k1PartitionKey.compareTo(k2PartitionKey);

    if (partitionComparison == 0) {
      int orderComparison = k1OrderKey.compareTo(k2OrderKey);

      if (orderComparison == 0) {
        if (k1OrderKeyBytes.length == 0) {
          return 0;
        } else {
          return 1;
        }
      }
System.err.println(orderComparison);
      return orderComparison;
    }

    return partitionComparison;
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
}
