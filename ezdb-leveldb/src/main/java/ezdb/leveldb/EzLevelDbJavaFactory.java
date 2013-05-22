package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

public class EzLevelDbJavaFactory implements EzLevelDbFactory {
  @Override
  public DB open(File path, Options options) throws IOException {
    return Iq80DBFactory.factory.open(path, options);
  }

  @Override
  public void destroy(File path, Options options) throws IOException {
    Iq80DBFactory.factory.destroy(path, options);
  }
}
