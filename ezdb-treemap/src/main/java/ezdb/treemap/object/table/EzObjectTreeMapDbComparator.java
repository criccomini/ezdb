package ezdb.treemap.object.table;

import java.util.Comparator;

import ezdb.util.Util;

public class EzObjectTreeMapDbComparator<H> implements Comparator<H> {

	private final Comparator<H> hashKeyComparator;

	public EzObjectTreeMapDbComparator(final Comparator<H> hashKeyComparator) {
		this.hashKeyComparator = hashKeyComparator;
	}

	@Override
	public int compare(final H a, final H b) {
		return Util.compareKeys(hashKeyComparator, a, b);
	}

}
