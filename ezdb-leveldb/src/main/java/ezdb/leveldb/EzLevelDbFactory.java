package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public interface EzLevelDbFactory {
  public DB open(File path, Options options) throws IOException;

  public void destroy(File path, Options options) throws IOException;
}
