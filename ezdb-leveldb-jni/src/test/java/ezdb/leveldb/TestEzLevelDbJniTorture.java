package ezdb.leveldb;

public class TestEzLevelDbJniTorture extends TestEzLevelDbTorture {
	
	@Override
	protected EzLevelDbFactory newFactory() {
		return new EzLevelDbJniFactory();
	}
}
