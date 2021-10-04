package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;

import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.serialization.Serializer;

public class EzLsmTreeDbJavaFactory implements EzLsmTreeDbFactory {

	protected StorageType newStorageType() {
		return StorageType.BLOCK_COMPRESSED;
	}

	protected CompressionCodec newCodec() {
		return new SnappyCodec();
	}

	protected <K, V> StoreBuilder<K, V> configure(final StoreBuilder<K, V> config) {
		return config;
	}

	@Override
	public void destroy(final File path) throws IOException {
		FileUtils.deleteQuietly(path);
	}

	@Override
	public <K, V> Store<K, V> open(final File path, final Serializer<K> keySerializer,
			final Serializer<V> valueSerializer, final Comparator<K> comparator) throws IOException {
		final StoreBuilder<K, V> config = new StoreBuilder<>(path, keySerializer, valueSerializer).setCodec(newCodec())
				.setStorageType(newStorageType());
		return configure(config).build();
	}

}
