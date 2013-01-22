package ezdb;

public interface TableRow<H, R, V> {
  public H getHashKey();

  public R getRangeKey();

  public V getValue();
}
