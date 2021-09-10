package ezdb.lmdb;

import java.io.File;
import java.io.IOException;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

import io.netty.buffer.ByteBuf;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLmDbFactory {
	public Env<ByteBuf> create(File path, EnvFlags... envFlags) throws IOException;

	public Dbi<ByteBuf> open(String tableName, Env<ByteBuf> env, EzLmDbComparator comparator, DbiFlags... dbiFlags)
			throws IOException;

	public void destroy(File path) throws IOException;
}
