package ezdb.comparator;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * A comparator that compares bytes using lexicographical ordering.
 * 
 * @author criccomini
 * 
 */
public class LexicographicalComparator implements Comparator<ByteBuffer> {
	public static final LexicographicalComparator get = new LexicographicalComparator();

	@Override
	public int compare(final ByteBuffer bytes1, final ByteBuffer bytes2) {
		return bytes1.compareTo(bytes2);
	}

}
