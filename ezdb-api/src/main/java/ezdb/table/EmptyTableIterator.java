package ezdb.table;

import java.util.NoSuchElementException;

import ezdb.util.TableIterator;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EmptyTableIterator<H, V> implements TableIterator<TableRow<H, V>> {

	private static final EmptyTableIterator INSTANCE = new EmptyTableIterator();

	public static <H, V> EmptyTableIterator<H, V> get() {
		return INSTANCE;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public TableRow<H, V> next() {
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
