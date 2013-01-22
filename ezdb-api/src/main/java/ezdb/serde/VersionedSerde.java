package ezdb.serde;

import java.util.Arrays;
import ezdb.DbException;
import ezdb.serde.VersionedSerde.Versioned;

public class VersionedSerde<O> implements Serde<Versioned<O>> {
  public static final int VERSION_NUMBER_BYTE_SIZE = 8;

  private final Serde<O> objectSerde;

  public VersionedSerde(Serde<O> objectSerde) {
    this.objectSerde = objectSerde;
  }

  @Override
  public Versioned<O> fromBytes(byte[] bytes) {
    byte[] bytesWithoutVersion = Arrays.copyOfRange(bytes, 0, bytes.length - VERSION_NUMBER_BYTE_SIZE);
    byte[] versionBytes = Arrays.copyOfRange(bytes, bytes.length - VERSION_NUMBER_BYTE_SIZE, bytes.length);
    return new Versioned<O>(objectSerde.fromBytes(bytesWithoutVersion), LongSerde.get.fromBytes(versionBytes));
  }

  @Override
  public byte[] toBytes(Versioned<O> versioned) {
    byte[] bytesWithoutVersion = objectSerde.toBytes(versioned.getObj());
    byte[] versionBytes = LongSerde.get.toBytes(versioned.getVersion());
    byte[] bytes = new byte[bytesWithoutVersion.length + VERSION_NUMBER_BYTE_SIZE];
    System.arraycopy(bytesWithoutVersion, 0, bytes, 0, bytesWithoutVersion.length);
    System.arraycopy(versionBytes, 0, bytes, bytesWithoutVersion.length, VERSION_NUMBER_BYTE_SIZE);
    return bytes;
  }

  public static class Versioned<O> {
    private final O obj;
    private final long version;

    public Versioned(O obj, long version) {
      this.obj = obj;
      this.version = version;
    }

    public O getObj() {
      return obj;
    }

    public long getVersion() {
      return version;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((obj == null) ? 0 : obj.hashCode());
      result = prime * result + (int) (version ^ (version >>> 32));
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
      Versioned other = (Versioned) obj;
      if (this.obj == null) {
        if (other.obj != null)
          return false;
      } else if (!this.obj.equals(other.obj))
        return false;
      if (version != other.version)
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "Versioned [obj=" + obj + ", version=" + version + "]";
    }
  }
}
