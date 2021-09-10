package ezdb.lmdb;

import java.io.File;
import java.io.IOException;

import org.lmdbjava.ByteBufProxy;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Env.Builder;
import org.lmdbjava.EnvFlags;

import ezdb.lmdb.util.FileUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

public class EzLmDbJnrFactory implements EzLmDbFactory {

	private static PooledByteBufAllocator DIRECT_ALLOCATOR;

	private static synchronized PooledByteBufAllocator getDirectAllocator() {
		if (DIRECT_ALLOCATOR == null) {
			if (PooledByteBufAllocator.defaultPreferDirect()) {
				DIRECT_ALLOCATOR = PooledByteBufAllocator.DEFAULT;
			} else {
				DIRECT_ALLOCATOR = new PooledByteBufAllocator();
			}
		}
		return DIRECT_ALLOCATOR;
	}

	@Override
	public Env<ByteBuf> create(final File path, final EnvFlags... envFlags) throws IOException {
		final Env<ByteBuf> env = newEnv().open(path.getAbsoluteFile(), envFlags);
		return env;
	}

	protected Builder<ByteBuf> newEnv() {
		return Env.create(ByteBufProxy.PROXY_NETTY).setMaxDbs(1).setMapSize(newMapSize())
				.setMaxReaders(Integer.MAX_VALUE);
	}

	protected long newMapSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Dbi<ByteBuf> open(final String tableName, final Env<ByteBuf> env, final EzLmDbComparator comparator,
			final DbiFlags... dbiFlags) throws IOException {
		final Dbi<ByteBuf> dbi = env.openDbi(tableName, comparator, dbiFlags);
		return dbi;
	}

	@Override
	public ByteBufAllocator getAllocator() {
		return getDirectAllocator();
	}

	@Override
	public void destroy(final File path) throws IOException {
		// implementation taken from java port of leveldb
		FileUtils.deleteRecursively(path);
	}

}
