package ezdb.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import com.indeed.lsmtree.core.Store;
import com.indeed.util.serialization.Serializer;

/**
 * An interface that allows us to inject either a JNI or pure-Java
 * implementation of LevelDB. EzLevelDb uses this class to open and delete
 * LevelDb instances.
 * 
 * @author criccomi
 * 
 */
public interface EzLsmTreeDbFactory {

	public <K, V> Store<K, V> open(File path, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
			Comparator<K> comparator) throws IOException;

	public void destroy(File path) throws IOException;
}
