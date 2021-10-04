package ezdb.util;

import java.util.Map.Entry;

import ezdb.table.TableRow;

public final class ObjectTableRow<H, V> implements TableRow<H, V> {

	private H hashKey;
	private V value;

	public ObjectTableRow(final Entry<H, V> entry) {
		this(entry.getKey(), entry.getValue());
	}

	public ObjectTableRow(final H hashKey, final V value) {
		this.hashKey = hashKey;
		this.value = value;
	}

	@Override
	public H getHashKey() {
		return hashKey;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getHashKey() == null) ? 0 : getHashKey().hashCode());
		result = prime * result + ((getValue() == null) ? 0 : getValue().hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final ObjectTableRow other = (ObjectTableRow) obj;
		if (getHashKey() == null) {
			if (other.getHashKey() != null) {
				return false;
			}
		} else if (!getHashKey().equals(other.getHashKey())) {
			return false;
		}
		if (getValue() == null) {
			if (other.getValue() != null) {
				return false;
			}
		} else if (!getValue().equals(other.getValue())) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [hashKey=" + getHashKey() + ", value=" + getValue() + "]";
	}

}
