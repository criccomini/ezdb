package ezdb.leveldb;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import ezdb.TableRow;
import ezdb.serde.Serde;

public class EzLevelDbTableRow<H, R, V> implements TableRow<H, R, V> {
  private final H hashKey;
  private final R rangeKey;
  private final V value;

  public EzLevelDbTableRow(H hashKey, R rangeKey, V value) {
    this.hashKey = hashKey;
    this.rangeKey = rangeKey;
    this.value = value;
  }

  public EzLevelDbTableRow(
      Entry<byte[], byte[]> rawRow,
      Serde<H> hashKeySerde,
      Serde<R> rangeKeySerde,
      Serde<V> valueSerde) {
    // TODO could make serde lazy for a bit of extra speed
    byte[] compoundKeyBytes = rawRow.getKey();

    ByteBuffer keyBuffer = ByteBuffer.wrap(compoundKeyBytes);
    int hashKeyBytesLength = keyBuffer.getInt();
    byte[] hashKeyBytes = new byte[hashKeyBytesLength];
    keyBuffer.get(hashKeyBytes);
    int rangeKeyBytesLength = keyBuffer.getInt();

    if (rangeKeyBytesLength > 0) {
      byte[] rangeKeyBytes = new byte[rangeKeyBytesLength];
      keyBuffer.get(rangeKeyBytes);
      rangeKey = rangeKeySerde.fromBytes(rangeKeyBytes);
    } else {
      rangeKey = null;
    }

    hashKey = hashKeySerde.fromBytes(hashKeyBytes);
    value = valueSerde.fromBytes(rawRow.getValue());
  }

  @Override
  public H getHashKey() {
    return hashKey;
  }

  @Override
  public R getRangeKey() {
    return rangeKey;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((hashKey == null) ? 0 : hashKey.hashCode());
    result = prime * result + ((rangeKey == null) ? 0 : rangeKey.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EzLevelDbTableRow other = (EzLevelDbTableRow) obj;
    if (hashKey == null) {
      if (other.hashKey != null)
        return false;
    } else if (!hashKey.equals(other.hashKey))
      return false;
    if (rangeKey == null) {
      if (other.rangeKey != null)
        return false;
    } else if (!rangeKey.equals(other.rangeKey))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "LevelDbHashRangeTableRow [hashKey=" + hashKey + ", rangeKey=" + rangeKey + ", value=" + value + "]";
  }
}
