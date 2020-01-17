package ezdb.util;

import java.util.Map.Entry;

import ezdb.TableRow;

public final class ObjectTableRow<H, R, V> implements TableRow<H, R, V> {

	private H hashKey;
	private R rangeKey;
	private V value;

	public ObjectTableRow(Entry<ObjectTableKey<H, R>, V> entry) {
		this(entry.getKey(), entry.getValue());
	}

	public ObjectTableRow(ObjectTableKey<H, R> key, V value) {
		this(key.getHashKey(), key.getRangeKey(), value);
	}

	public ObjectTableRow(H hashKey, R rangeKey, V value) {
		this.hashKey = hashKey;
		this.rangeKey = rangeKey;
		this.value = value;
	}

	@Override
	public H getHashKey() {
		return hashKey;
	}

	@Override
	public R getRangeKey() {
		return rangeKey;
	}

	@Override
	public V getValue() {
		return value;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((getHashKey() == null) ? 0 : getHashKey().hashCode());
		result = prime * result
				+ ((getRangeKey() == null) ? 0 : getRangeKey().hashCode());
		result = prime * result
				+ ((getValue() == null) ? 0 : getValue().hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ObjectTableRow other = (ObjectTableRow) obj;
		if (getHashKey() == null) {
			if (other.getHashKey() != null)
				return false;
		} else if (!getHashKey().equals(other.getHashKey()))
			return false;
		if (getRangeKey() == null) {
			if (other.getRangeKey() != null)
				return false;
		} else if (!getRangeKey().equals(other.getRangeKey()))
			return false;
		if (getValue() == null) {
			if (other.getValue() != null)
				return false;
		} else if (!getValue().equals(other.getValue()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [hashKey=" + getHashKey()
				+ ", rangeKey=" + getRangeKey() + ", value=" + getValue() + "]";
	}

}
