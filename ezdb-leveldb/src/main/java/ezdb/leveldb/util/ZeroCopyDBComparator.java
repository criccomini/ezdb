package ezdb.leveldb.util;

import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.util.Slice;

public interface ZeroCopyDBComparator extends DBComparator {

	int compare(final Slice k1, final Slice k2);

}
