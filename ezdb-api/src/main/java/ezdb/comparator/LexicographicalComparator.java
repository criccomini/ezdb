package ezdb.comparator;

import java.util.Comparator;

import io.netty.buffer.ByteBuf;

/**
 * A comparator that compares bytes using lexicographical ordering.
 * 
 * @author criccomini
 * 
 */
public class LexicographicalComparator implements Comparator<ByteBuf> {
	public static final LexicographicalComparator get = new LexicographicalComparator();

	@Override
	public int compare(final ByteBuf bytes1, final ByteBuf bytes2) {
		return bytes1.nioBuffer().compareTo(bytes2.nioBuffer());
	}

}
