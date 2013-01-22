package ezdb.comparator;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * A comparator that compares bytes using lexicographical ordering.
 * 
 * @author criccomini
 * 
 */
public class LexicographicalComparator implements Comparator<byte[]> {
  public static final LexicographicalComparator get = new LexicographicalComparator();

  @Override
  public int compare(byte[] bytes1, byte[] bytes2) {
    return ByteBuffer.wrap(bytes1).compareTo(ByteBuffer.wrap(bytes2));
  }
}
