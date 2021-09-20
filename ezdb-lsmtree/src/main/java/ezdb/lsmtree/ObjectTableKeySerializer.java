package ezdb.lsmtree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import com.indeed.util.serialization.Serializer;

import ezdb.serde.Serde;
import ezdb.util.ObjectTableKey;
import ezdb.util.Util;

public class ObjectTableKeySerializer<H, R> implements Serializer<ObjectTableKey<H, R>> {

	private final EzdbSerializer<H> hashKeySerializer;
	private final EzdbSerializer<R> rangeKeySerializer;
	private final Comparator<ObjectTableKey<H, R>> comparator;

	public ObjectTableKeySerializer(final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Comparator<ObjectTableKey<H, R>> comparator) {
		if (hashKeySerde == null) {
			throw new NullPointerException("hashKeySerde should not be null");
		}
		if (rangeKeySerde == null) {
			throw new NullPointerException("rangeKeySerde should not be null");
		}
		if (comparator == null) {
			throw new NullPointerException("comparator should not be null");
		}
		this.hashKeySerializer = new EzdbSerializer<H>(hashKeySerde);
		this.rangeKeySerializer = new EzdbSerializer<R>(rangeKeySerde);
		this.comparator = comparator;
	}

	@Override
	public void write(final ObjectTableKey<H, R> t, final DataOutput out) throws IOException {
		hashKeySerializer.write(t.getHashKey(), out);
		rangeKeySerializer.write(t.getRangeKey(), out);
	}

	@Override
	public ObjectTableKey<H, R> read(final DataInput in) throws IOException {
		final H hashKey = hashKeySerializer.read(in);
		final R rangeKey = rangeKeySerializer.read(in);
		return Util.combine(hashKey, rangeKey, comparator);
	}

}
