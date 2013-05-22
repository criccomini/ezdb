package ezdb.leveldb;

import java.io.File;

import org.junit.Before;

public class TestEzLevelDbJniTorture extends TestEzLevelDbTorture {
  @Before
  public void before() {
    db = new EzLevelDb(new File("/tmp"), new EzLevelDbJniFactory());
  }
}
