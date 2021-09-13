package ezdb;

import java.util.NoSuchElementException;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EmptyTableIterator<H, R, V> implements TableIterator<H, R, V> {

	private static final EmptyTableIterator INSTANCE = new EmptyTableIterator();

	public static <H, R, V> EmptyTableIterator<H, R, V> get() {
		return INSTANCE;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public TableRow<H, R, V> next() {
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
