package ezdb.leveldb;

import java.io.File;
import org.junit.Before;
import ezdb.serde.IntegerSerde;

public class TestEzLevelDbJni extends TestEzLevelDb {
  @Before
  public void before() {
    ezdb = new EzLevelDb(new File("/tmp"), new EzLevelDbJniFactory());
    ezdb.deleteTable("test");
    table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
  }
}
