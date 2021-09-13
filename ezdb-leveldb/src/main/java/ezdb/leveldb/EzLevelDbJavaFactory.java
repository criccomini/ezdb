package ezdb.leveldb;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.impl.ExtendedDbImpl;
import org.iq80.leveldb.impl.Iq80DBFactory;

public class EzLevelDbJavaFactory implements EzLevelDbFactory {
	@Override
	public ExtendedDbImpl open(final File path, final Options options) throws IOException {
		return new ExtendedDbImpl(options, path.getAbsolutePath(), EnvImpl.createEnv());
	}

	@Override
	public void destroy(final File path, final Options options) throws IOException {
		Iq80DBFactory.factory.destroy(path, options);
	}
}
