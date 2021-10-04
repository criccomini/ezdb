package ezdb.lmdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.EnvFlags;

import ezdb.lmdb.util.FileUtils;

public class EzLmDbJnrFactory implements EzLmDbFactory {
	@Override
	public Env<ByteBuffer> create(final File path, final EnvFlags... envFlags) throws IOException {
		final Env<ByteBuffer> env = newEnv().open(path.getAbsoluteFile(), envFlags);
		return env;
	}

	protected Builder<ByteBuffer> newEnv() {
		return Env.create().setMaxDbs(1).setMapSize(newMapSize()).setMaxReaders(Integer.MAX_VALUE);
	}

	protected long newMapSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Dbi<ByteBuffer> open(final String tableName, final Env<ByteBuffer> env,
			final Comparator<ByteBuffer> comparator, final boolean rangeTable, final DbiFlags... dbiFlags)
			throws IOException {
		final Dbi<ByteBuffer> dbi = env.openDbi(tableName, comparator, dbiFlags);
		return dbi;
	}

	@Override
	public void destroy(final File path) throws IOException {
		// implementation taken from java port of leveldb
		FileUtils.deleteRecursively(path);
	}

}
