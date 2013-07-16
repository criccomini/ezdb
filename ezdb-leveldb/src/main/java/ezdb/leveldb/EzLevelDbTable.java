package ezdb.leveldb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import ezdb.DbException;
import ezdb.RangeTable;
import ezdb.RawTableRow;
import ezdb.TableIterator;
import ezdb.TableRow;
import ezdb.serde.Serde;
import ezdb.util.Util;

public class EzLevelDbTable<H, R, V> implements RangeTable<H, R, V> {
  private final DB db;
  private final Serde<H> hashKeySerde;
  private final Serde<R> rangeKeySerde;
  private final Serde<V> valueSerde;
  private final Comparator<byte[]> hashKeyComparator;
  private final Comparator<byte[]> rangeKeyComparator;

  public EzLevelDbTable(File path, EzLevelDbFactory factory, Serde<H> hashKeySerde, Serde<R> rangeKeySerde,
      Serde<V> valueSerde, Comparator<byte[]> hashKeyComparator, Comparator<byte[]> rangeKeyComparator) {
    this.hashKeySerde = hashKeySerde;
    this.rangeKeySerde = rangeKeySerde;
    this.valueSerde = valueSerde;
    this.hashKeyComparator = hashKeyComparator;
    this.rangeKeyComparator = rangeKeyComparator;

    Options options = new Options();
    options.createIfMissing(true);
    options.comparator(new EzLevelDbComparator(hashKeyComparator, rangeKeyComparator));

    try {
      this.db = factory.open(path, options);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  public void put(H hashKey, V value) {
    put(hashKey, null, value);
  }

  @Override
  public void put(H hashKey, R rangeKey, V value) {
    db.put(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey), valueSerde.toBytes(value));
  }

  @Override
  public V get(H hashKey) {
    return get(hashKey, null);
  }

  @Override
  public V get(H hashKey, R rangeKey) {
    byte[] valueBytes = db.get(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));

    if (valueBytes == null) {
      return null;
    }

    return valueSerde.fromBytes(valueBytes);
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
    iterator.seek(keyBytesFrom);
    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext()
            && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    iterator.seek(keyBytesFrom);
    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext()
            && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  @Override
  public TableIterator<H, R, V> range(H hashKey, R fromRangeKey, R toRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
    iterator.seek(keyBytesFrom);
    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext()
            && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, iterator.peekNext().getKey()) > 0;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return new RawTableRow<H, R, V>(iterator.next(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  public TableIterator<H, R, V> rangeReverse(final H hashKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, null);
    iterator.seek(keyBytesFrom);
    Entry<byte[], byte[]> last = null;
    while (iterator.hasNext()
        && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekNext().getKey()) == 0) {
      last = iterator.next();
    }
    // if there is no last one, there is nothing at all in the table
    if (last == null) {
      return new TableIterator<H, R, V>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public TableRow<H, R, V> next() {
          throw new NoSuchElementException();
        }

        @Override
        public void remove() {
          throw new NoSuchElementException();
        }

        @Override
        public void close() {
        }
      };
    }
    // since last has been found, seek again for that one
    iterator.seek(last.getKey());

    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

      private boolean fixFirst = true;

      @Override
      public boolean hasNext() {
        if (useFixFirst()) {
          return true;
        }
        return iterator.hasPrev()
            && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekPrev().getKey()) == 0;
      }

      private boolean useFixFirst() {
        if (fixFirst && iterator.hasNext()) {
          fixFirst = false;
          final Entry<byte[], byte[]> peekNext = iterator.peekNext();
          if (peekNext != null) {
            if (Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekNext.getKey()) == 0) {
              return true;
            } else {
              fixFirst = false;
            }
          }
        }
        return false;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (useFixFirst()) {
          return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
        }
        return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        if (useFixFirst()) {
          throw new UnsupportedOperationException("Not possible on first result for now...");
        }
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (final Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    iterator.seek(keyBytesFrom);
    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

      private boolean fixFirst = true;

      @Override
      public boolean hasNext() {
        if (useFixFirst()) {
          return true;
        }
        return iterator.hasPrev()
            && Util.compareKeys(hashKeyComparator, null, keyBytesFrom, iterator.peekPrev().getKey()) == 0;
      }

      private boolean useFixFirst() {
        if (fixFirst && iterator.hasNext()) {
          fixFirst = false;
          final Entry<byte[], byte[]> peekNext = iterator.peekNext();
          if (peekNext != null) {
            if (Util.compareKeys(hashKeyComparator, null, keyBytesFrom, peekNext.getKey()) == 0) {
              return true;
            } else {
              fixFirst = false;
            }
          }
        }
        return false;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (useFixFirst()) {
          return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
        }
        return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        if (useFixFirst()) {
          throw new UnsupportedOperationException("Not possible on first result for now...");
        }
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (final Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  public TableIterator<H, R, V> rangeReverse(final H hashKey, final R fromRangeKey, final R toRangeKey) {
    final DBIterator iterator = db.iterator();
    final byte[] keyBytesFrom = Util.combine(hashKeySerde, rangeKeySerde, hashKey, fromRangeKey);
    final byte[] keyBytesTo = Util.combine(hashKeySerde, rangeKeySerde, hashKey, toRangeKey);
    iterator.seek(keyBytesFrom);
    return new AutoClosingTableIterator<H, R, V>(new TableIterator<H, R, V>() {

      private boolean fixFirst = true;

      @Override
      public boolean hasNext() {
        if (useFixFirst()) {
          return true;
        }
        return iterator.hasPrev()
            && Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, iterator.peekPrev().getKey()) < 0;
      }

      private boolean useFixFirst() {
        if (fixFirst && iterator.hasNext()) {
          fixFirst = false;
          final Entry<byte[], byte[]> peekNext = iterator.peekNext();
          if (peekNext != null) {
            if (Util.compareKeys(hashKeyComparator, rangeKeyComparator, keyBytesTo, peekNext.getKey()) <= 0) {
              return true;
            } else {
              fixFirst = false;
            }
          }
        }
        return false;
      }

      @Override
      public TableRow<H, R, V> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        if (useFixFirst()) {
          return new RawTableRow<H, R, V>(iterator.peekNext(), hashKeySerde, rangeKeySerde, valueSerde);
        }
        return new RawTableRow<H, R, V>(iterator.prev(), hashKeySerde, rangeKeySerde, valueSerde);
      }

      @Override
      public void remove() {
        if (useFixFirst()) {
          throw new UnsupportedOperationException("Not possible on first result for now...");
        }
        iterator.remove();
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } catch (final Exception e) {
          throw new DbException(e);
        }
      }
    });
  }

  @Override
  public void delete(H hashKey) {
    delete(hashKey, null);
  }

  @Override
  public void delete(H hashKey, R rangeKey) {
    this.db.delete(Util.combine(hashKeySerde, rangeKeySerde, hashKey, rangeKey));
  }

  @Override
  public void close() {
    try {
      this.db.close();
    } catch (Exception e) {
      throw new DbException(e);
    }
  }

  private static class AutoClosingTableIterator<_H, _R, _V> implements TableIterator<_H, _R, _V> {

    private final TableIterator<_H, _R, _V> delegate;
    private boolean closed;

    public AutoClosingTableIterator(final TableIterator<_H, _R, _V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      final boolean hasNext = delegate.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    }

    @Override
    public TableRow<_H, _R, _V> next() {
      if (closed) {
        throw new NoSuchElementException();
      }
      return delegate.next();
    }

    @Override
    public void remove() {
      delegate.remove();
    }

    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      close();
    }

    @Override
    public void close() {
      closed = true;
      delegate.close();
    }

  }
}
