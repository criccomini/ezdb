package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.fusesource.leveldbjni.JniDBFactory;

public class EzLevelDbJniFactory implements EzLevelDbFactory {
  @Override
  public DB open(File path, Options options) throws IOException {
    return JniDBFactory.factory.open(path, options);
  }

  @Override
  public void destroy(File path, Options options) throws IOException {
    JniDBFactory.factory.destroy(path, options);
  }
}
