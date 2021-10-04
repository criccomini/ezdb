package ezdb.treemap.object.table.range;

import java.util.Comparator;

import ezdb.util.ObjectRangeTableKey;
import ezdb.util.Util;

public class EzObjectTreeMapDbRangeComparator<H, R> implements Comparator<ObjectRangeTableKey<H, R>> {

	private final Comparator<H> hashKeyComparator;
	private final Comparator<R> rangeKeyComparator;

	public EzObjectTreeMapDbRangeComparator(final Comparator<H> hashKeyComparator, final Comparator<R> rangeKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
		this.rangeKeyComparator = rangeKeyComparator;
	}

	@Override
	public int compare(final ObjectRangeTableKey<H, R> a, final ObjectRangeTableKey<H, R> b) {
		return Util.compareKeys(hashKeyComparator, rangeKeyComparator, a, b);
	}

}
