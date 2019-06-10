package ezdb.leveldb;

public class TestEzLevelDbJni extends TestEzLevelDb {
	
	@Override
	protected EzLevelDbFactory newFactory() {
		return new EzLevelDbJniFactory();
	}

}
