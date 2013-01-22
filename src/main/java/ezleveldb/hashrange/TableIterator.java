package ezleveldb.hashrange;

import java.util.Iterator;

public interface TableIterator<H, R, V> extends Iterator<TableRow<H, R, V>> {
  public void close();
}
