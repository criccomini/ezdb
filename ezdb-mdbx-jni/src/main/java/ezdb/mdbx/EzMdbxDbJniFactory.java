package ezdb.mdbx;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

import ezdb.mdbx.util.FileUtils;

public class EzMdbxDbJniFactory implements EzMdbxDbFactory {
	@Override
	public Env<ByteBuffer> create(File path, EnvFlags... envFlags) throws IOException {
		Env<ByteBuffer> env = Env.create().setMaxDbs(1).setMapSize(10485760).setMaxReaders(Integer.MAX_VALUE).open(path.getAbsoluteFile(), envFlags);
		return env;
	}
	@Override
	public Dbi<ByteBuffer> open(String tableName, Env<ByteBuffer> env, EzMdbxDbComparator comparator, DbiFlags... dbiFlags) throws IOException {
		Dbi<ByteBuffer> dbi = env.openDbi(tableName, comparator, dbiFlags);
		return dbi;
	}

	@Override
	public void destroy(File path) throws IOException {
		//implementation taken from java port of leveldb
		FileUtils.deleteRecursively(path);
	}


}
