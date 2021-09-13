package ezdb.leveldb;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.ExtendedDbImpl;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLevelDbFactory {
	public ExtendedDbImpl open(File path, Options options) throws IOException;

	public void destroy(File path, Options options) throws IOException;
}
