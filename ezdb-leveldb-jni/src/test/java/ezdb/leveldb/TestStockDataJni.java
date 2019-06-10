package ezdb.leveldb;

public class TestStockDataJni extends TestStockData {

	@Override
	protected EzLevelDbFactory newFactory() {
		return new EzLevelDbJniFactory();
	}
	
}