package ezdb.treemap.object;

import java.util.Comparator;

import ezdb.util.ObjectTableKey;
import ezdb.util.Util;

public class EzObjectTreeMapDbComparator<H, R> implements Comparator<ObjectTableKey<H, R>> {

	private final Comparator<H> hashKeyComparator;
	private final Comparator<R> rangeKeyComparator;

	public EzObjectTreeMapDbComparator(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final ObjectTableKey<H, R> a, final ObjectTableKey<H, R> b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a, b);
	}

}