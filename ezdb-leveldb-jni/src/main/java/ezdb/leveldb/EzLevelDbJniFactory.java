package ezdb.leveldb;

import java.io.File;
import java.io.IOException;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class EzLevelDbJniFactory {
	public DB open(final File path, final Options options, final boolean rangeTable) throws IOException {
		return JniDBFactory.factory.open(path, options);
	}

	public void destroy(final File path, final Options options) throws IOException {
		JniDBFactory.factory.destroy(path, options);
	}
}
