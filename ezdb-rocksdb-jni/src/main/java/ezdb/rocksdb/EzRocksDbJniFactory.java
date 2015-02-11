package ezdb.rocksdb;

import java.io.File;
import java.io.IOException;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import ezdb.rocksdb.util.FileUtils;

public class EzRocksDbJniFactory implements EzRocksDbFactory {
	@Override
	public RocksDB open(File path, Options options) throws IOException {
		try {
			return RocksDB.open(options, path.getAbsolutePath());
		} catch (RocksDBException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void destroy(File path, Options options) throws IOException {
		//implementation taken from java port of leveldb
		FileUtils.deleteRecursively(path);
	}
}
