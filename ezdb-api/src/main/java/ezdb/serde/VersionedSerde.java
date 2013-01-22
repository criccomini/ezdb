package ezdb.serde;

import java.util.Arrays;
import ezdb.serde.VersionedSerde.Versioned;

/**
 * A wrapper serde that handles pre-pending some version number to a byte
 * payload. The format for the version byte array is:
 * 
 * <pre>
 * [4 byte version]
 * [arbitrary object bytes]
 * </pre>
 * 
 * @author criccomini
 * 
 * @param <O>
 *          The type of object that we wish to version.
 */
public class VersionedSerde<O> implements Serde<Versioned<O>> {
  public static final int VERSION_NUMBER_BYTE_SIZE = 8;

  private final Serde<O> objectSerde;

  public VersionedSerde(Serde<O> objectSerde) {
    this.objectSerde = objectSerde;
  }

  @Override
  public Versioned<O> fromBytes(byte[] bytes) {
    byte[] versionBytes = Arrays.copyOfRange(bytes, 0, VERSION_NUMBER_BYTE_SIZE);
    byte[] bytesWithoutVersion = Arrays.copyOfRange(bytes, VERSION_NUMBER_BYTE_SIZE, bytes.length);
    return new Versioned<O>(objectSerde.fromBytes(bytesWithoutVersion), LongSerde.get.fromBytes(versionBytes));
  }

  @Override
  public byte[] toBytes(Versioned<O> versioned) {
    byte[] versionBytes = LongSerde.get.toBytes(versioned.getVersion());
    byte[] bytesWithoutVersion = objectSerde.toBytes(versioned.getObj());
    byte[] bytes = new byte[bytesWithoutVersion.length + VERSION_NUMBER_BYTE_SIZE];
    System.arraycopy(versionBytes, 0, bytes, 0, VERSION_NUMBER_BYTE_SIZE);
    System.arraycopy(bytesWithoutVersion, 0, bytes, VERSION_NUMBER_BYTE_SIZE, bytesWithoutVersion.length);
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
