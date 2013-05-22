package ezdb.leveldb;

import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLevelDbFactory {
  public DB open(File path, Options options) throws IOException;

  public void destroy(File path, Options options) throws IOException;
}
