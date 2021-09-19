package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;

import com.indeed.lsmtree.core.Store;
import com.indeed.util.serialization.Serializer;

import ezdb.util.ObjectTableKey;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLsmTreeDbFactory {

	public <H, R, V> Store<ObjectTableKey<H, R>, V> open(File path,
			final Serializer<ObjectTableKey<H, R>> keySerializer, final Serializer<V> valueSerializer,
			EzLsmTreeDbComparator<H, R> comparator) throws IOException;

	public void destroy(File path) throws IOException;
}
