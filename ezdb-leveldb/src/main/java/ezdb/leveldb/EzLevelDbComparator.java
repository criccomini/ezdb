package ezdb.leveldb;

import java.util.Comparator;
import org.iq80.leveldb.DBComparator;
import ezdb.util.Util;

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
    return Util.compareKeys(hashKeyComparator, rangeKeyComparator, k1, k2);
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
