package ezdb;

public interface Table<H, R, V> {
  public void put(H hashKey, V value);

  public void put(H hashKey, R rangeKey, V value);

  public V get(H hashKey);

  public V get(H hashKey, R rangeKey);

  public TableIterator<H, R, V> range(H hashKey);

  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey);

  public void delete(H hashKey);

  public void delete(H hashKey, R rangeKey);

  public void close();
}
