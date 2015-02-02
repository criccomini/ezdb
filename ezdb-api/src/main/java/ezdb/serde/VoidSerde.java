package ezdb.serde;


public final class VoidSerde implements Serde<Void> {

    public static final VoidSerde get = new VoidSerde();

    private VoidSerde() {}

    @Override
    public Void fromBytes(final byte[] paramArrayOfByte) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toBytes(final Void paramO) {
        throw new UnsupportedOperationException();
    }

}
