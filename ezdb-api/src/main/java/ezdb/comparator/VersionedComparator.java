package ezdb.comparator;

import java.util.Arrays;
import java.util.Comparator;
import ezdb.serde.VersionedSerde;

/**
 * A comparator that strips out a trailing long byte array, and defers the
 * comparison of the remaining bytes to a wrapped comparator.
 * 
 * @author criccomini
 * 
 */
public class VersionedComparator implements Comparator<byte[]> {
  private final Comparator<byte[]> wrappedComparator;

  public VersionedComparator() {
    this.wrappedComparator = new LexicographicalComparator();
  }

  /**
   * @param wrappedComparator
   *          The underlying comparator to use once the trailing version bytes
   *          have been stripped from the byte array.
   */
  public VersionedComparator(Comparator<byte[]> wrappedComparator) {
    this.wrappedComparator = wrappedComparator;
  }

  @Override
  public int compare(byte[] bytes1, byte[] bytes2) {
    byte[] bytes1WithoutVersion = new byte[0];
    byte[] bytes2WithoutVersion = new byte[0];

    if (bytes1 != null && bytes1.length > 0) {
      bytes1WithoutVersion = Arrays.copyOfRange(bytes1, 0, bytes1.length - VersionedSerde.VERSION_NUMBER_BYTE_SIZE);
    }

    if (bytes2 != null && bytes2.length > 0) {
      bytes2WithoutVersion = Arrays.copyOfRange(bytes2, 0, bytes2.length - VersionedSerde.VERSION_NUMBER_BYTE_SIZE);
    }

    return wrappedComparator.compare(bytes1WithoutVersion, bytes2WithoutVersion);
  }
}
