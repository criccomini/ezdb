package ezdb;

public class DbException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public DbException() {
    super();
  }

  public DbException(String s, Throwable t) {
    super(s, t);
  }

  public DbException(String s) {
    super(s);
  }

  public DbException(Throwable t) {
    super(t);
  }

}