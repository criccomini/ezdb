# EZDB

EZDB provides a nice Java wrapper around LevelDB that provides:

* Key/value lookup
* Hash/range lookup (like Amazon's DynamoDB)
* Pluggable serializers
* Pluggable range key sorting
* Basic versioning for values

### Using EZDB

To use EZDB, you need to create a database:

    Db ezdb = new EzLevelDb(new File("/tmp"));

The database is the place where all of your tables will be stored. Each "table" is actually just a LevelDB database. The file that's passed into EzLevelDB specifies a root folder where LevelDB databases will be stored.

##### Key/Value

Once you have a database, you can create a table:

    Table<Integer, Integer> table = ezdb.getTable("simple", IntegerSerde.get, IntegerSerde.get);

In this example, we're just creating a simple key/value table. If "simple" already exists on disk, it will be re-used. If not, it will be created. A "serde" (serializer/deserializer) is supplied to translate your Java objects to and from byte[] arrays that LevelDB can handle.

Now you can store data in your table:

    table.put(1, 2);

And read data back out:

    System.out.println(table.get(1));

This would print the number 2.

##### Hash/Range

EZDB also supports hash/range tables. These tables provide range query functionality, so you can scan rows, instead of just doing a simple lookup. First, you need to get a hash/range table:

    RangeTable<Integer, String, Integer> table = ezdb.getTable("hash-range", IntegerSerde.get, StringSerde.get, IntegerSerde.get);

With hash/range tables, you supply three serdes when getting the table; one for the hash key, range key, and value, respectively.

Now you can store data in the table:

    table.put(1213, "20120101-bang", 1357);
    table.put(1213, "20120102-foo", 1234);
    table.put(1213, "20120102-bar", 5678);
    table.put(2324, "20120102-baz", 2468);
    table.put(1213, "20120103-baz", 3579);
    table.put(1213, 12345678);

Notice that you can use a hash/range table flexibly as both a key/value store and a hash/range store. In the last line, we've stored a simple key/value. 

    System.out.println(table.get(1213));

This would print 12345678. If you don't supply a range key, EZDB just defaults to null.

You can always do simple lookups by hash/range:

    System.out.println(table.get(1213, "20120103-baz"));

This would print 2468.

You can also do a range query within a hash bucket.

    TableIterator<Integer, String, Integer> it = table.range(1213, "20120102", "20120103");
    
    while(it.hasNext()) {
      System.out.println(it.next().getValue());
    }

This would print 5678 then 1234 ("20120102-bar" is lexicographically less than "20120102-foo"), but not 1357, 2468, 3579, or 12345678. This functionality is very similar to DynamoDB's hash key/range key behavior. Using the hash key, you can group rows together, and then perform range queries within these buckets! Pretty cool, huh?

##### Pluggable Range Key Comparators

EZDB also lets you customize the way that your range keys are sorted for a table. Here's an example of how to plug in a comparator that sorts your range keys in reverse order:

    Table<Integer, Integer, Integer> table = ezdb.getTable("range-keys-comparators", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        // Let's do things in reverse lexicographical order.
        return -1 * ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
      }
    });

##### Versioned Values

EZDB also provides some utility classes to let you version your data. EZDB only persists the "latest" version of your data, but it can be useful to tag your data with some version number for future lookups. To user versioning, just wrap your value's serde in a VersionSerde.

    RangeTable<Integer, String, Versioned<Integer>> table = ezdb.getTable("test-versioned-values", IntegerSerde.get, StringSerde.get, new VersionedSerde<Integer>(IntegerSerde.get));

Then, when you put data, put your values in a Versioned wrapper.

    table.put(1213, "foo", new Versioned<Integer>(2, 0));
    table.put(1213, "foo", new Versioned<Integer>(3, 1));
    table.put(1213, new Versioned<Integer>(12345678, 0));

You do your lookups as normal.

    System.out.println(table.get(1213, "foo"))

This would print obj=3, version=1. The get() method returns a Versioned wrapper with your data and version number inside of it.

### Building EZDB

Building EZDB is built with Maven:

    mvn clean package

### Using EZDB

EZDB is published to maven central. You can pull it in with:

    <dependency>
      <groupId>com.github.criccomini</groupId>
      <artifactId>ezdb-leveldb</artifactId>
      <version>0.1.5</version>
    </dependency>

### Java Documentation

You can get the javadocs for EZDB here:

http://criccomini.github.com/ezdb/javadocs
