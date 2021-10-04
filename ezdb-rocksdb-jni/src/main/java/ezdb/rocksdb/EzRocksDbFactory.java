package ezdb.rocksdb;

import java.io.File;
import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzRocksDbFactory {
	public RocksDB open(File path, Options options, boolean rangeTable) throws IOException;

	public void destroy(File path, Options options) throws IOException;
}
