package ezdb.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLmDbFactory {
	public Env<ByteBuffer> create(File path, EnvFlags... envFlags) throws IOException;

	public Dbi<ByteBuffer> open(String tableName, Env<ByteBuffer> env, Comparator<ByteBuffer> comparator,
			DbiFlags... dbiFlags) throws IOException;

	public void destroy(File path) throws IOException;
}
