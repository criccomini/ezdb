package ezleveldb.serde;

public interface Serde<O> {
  public O fromBytes(byte[] bytes);

  public byte[] toBytes(O obj);
}
