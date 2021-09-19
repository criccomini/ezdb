package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.GzipCodec;
import com.indeed.util.serialization.Serializer;

import ezdb.util.ObjectTableKey;

public class EzLsmTreeDbJavaFactory implements EzLsmTreeDbFactory {

	protected StorageType newStorageType() {
		return StorageType.BLOCK_COMPRESSED;
	}

	protected CompressionCodec newCodec() {
		return new GzipCodec();
	}

	protected <H, R, V> StoreBuilder<ObjectTableKey<H, R>, V> configure(
			final StoreBuilder<ObjectTableKey<H, R>, V> config) {
		return config;
	}

	@Override
	public void destroy(final File path) throws IOException {
		FileUtils.deleteQuietly(path);
	}

	@Override
	public <H, R, V> Store<ObjectTableKey<H, R>, V> open(final File path,
			final Serializer<ObjectTableKey<H, R>> keySerializer, final Serializer<V> valueSerializer,
			final EzLsmTreeDbComparator<H, R> comparator) throws IOException {
		final StoreBuilder<ObjectTableKey<H, R>, V> config = new StoreBuilder<>(path, keySerializer, valueSerializer)
				.setCodec(newCodec()).setStorageType(newStorageType());
		return configure(config).build();
	}

}
