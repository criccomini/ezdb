package ezdb.table;

/**
 * A representation of a row in a table.
 * 
 * @author criccomini
 * 
 * @param <H> The type of the hash key for this key/value table.
 * @param <R> The type of the range key for this key/value table.
 * @param <V> The type of the value for this key/value table.
 */
public interface RangeTableRow<H, R, V> extends TableRow<H, V> {

	/**
	 * @return The range key for this row. If no range key exists, null should be
	 *         returned.
	 */
	public R getRangeKey();

}
