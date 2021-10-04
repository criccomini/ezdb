package ezdb.table.range;

import java.util.NoSuchElementException;

import ezdb.table.RangeTableRow;
import ezdb.util.TableIterator;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EmptyRangeTableIterator<H, R, V> implements TableIterator<RangeTableRow<H, R, V>> {

	private static final EmptyRangeTableIterator INSTANCE = new EmptyRangeTableIterator();

	public static <H, R, V> EmptyRangeTableIterator<H, R, V> get() {
		return INSTANCE;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public RangeTableRow<H, R, V> next() {
		throw new NoSuchElementException();
	}

	@Override
	public void remove() {
		throw new NoSuchElementException();
	}

	@Override
	public void close() {
	}

}
