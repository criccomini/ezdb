package ezdb.rocksdb;

import java.io.File;
import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import ezdb.rocksdb.util.FileUtils;

public class EzRocksDbJniFactory implements EzRocksDbFactory {
	@Override
	public RocksDB open(final File path, final Options options, final boolean rangeTable) throws IOException {
		try {
			return RocksDB.open(options, path.getAbsolutePath());
		} catch (final RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void destroy(final File path, final Options options) throws IOException {
		// implementation taken from java port of leveldb
		FileUtils.deleteRecursively(path);
	}
}
