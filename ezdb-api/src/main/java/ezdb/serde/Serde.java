package ezdb.serde;

/**
 * A generic serializer that can be used to convert Java objects back and forth
 * to byte arrays.
 * 
 * @author criccomini
 * 
 * @param <O>
 *          The type of the object that a serde can convert.
 */
public interface Serde<O> {
  public O fromBytes(byte[] bytes);

  public byte[] toBytes(O obj);
}
