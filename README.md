# EZDB

EZDB provides a nice Java wrapper around LevelDB. Let's take a look!

##### Simple Key/Value

You can always use EZDB as a regular key/value store.

    Db ezdb = new EzLevelDb(new File("/tmp"));
    Table<Integer, Integer> table = ezdb.getTable("simple", IntegerSerde.get, IntegerSerde.get);
    
    table.put(1, 1);
    
    // Prints 1.
    System.out.println(table.get(1));

##### Hash/Range Combination

EZDB also supports hash/range lookups!

    RangeTable<Integer, String, Integer> table = ezdb.getTable("hash-range", IntegerSerde.get, StringSerde.get, IntegerSerde.get);
    
    table.put(1213, "20120101-bang", 1357);
    table.put(1213, "20120102-foo", 1234);
    table.put(1213, "20120102-bar", 5678);
    table.put(1213, "20120103-baz", 2468);
    table.put(1213, 12345678);
    
    // Prints 2468.
    System.out.println(table.get(1213, "20120103-baz"));
    
    // Let's do a range query from January 2nd (inclusive) to January 3rd (exclusive).
    TableIterator<Integer, String, Integer> it = table.range(1213, "20120102", "20120103");
    
    // Prints 5678 then 1234, but not 1357, 2468, or 12345678.
    while(it.hasNext()) {
      System.out.println(it.next().getValue());
    }
    
    // Prints 12345678.
    System.out.println(table.get(1213));

This functionality is very similar to DynamoDB's hash key/range key behavior. Using the hash key, you can group rows together, and then perform range queries within these buckets! Pretty cool, huh?

##### Pluggable Serialization

EZDB allows you to plug in your own serializations. The examples above show IntegerSerde and StringSerde, but you can plug in anything you want. The only thing that you need to be mindful of is how bytes are sorted (they default to lexicographical sorting) when plugging in a custom range serializer, as this will affect the sort order of your range queries.

##### Pluggable Range Key Comparators

EZDB also supports custom range key comparators. By default, everything is sorted lexicographically, but you can always change range key sorting to suit your needs. Here's an example that sorts things backwards.

    Table<Integer, Integer, Integer> table = ezdb.getTable("range-keys-comparators", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get, new Comparator<byte[]>() {
      @Override
      public int compare(byte[] o1, byte[] o2) {
        // Let's do things in reverse lexicographical order.
        return -1 * ByteBuffer.wrap(o1).compareTo(ByteBuffer.wrap(o2));
      }
    });

    table.put(1, 1, 100);
    table.put(1, 2, 200);
    table.put(1, 3, 300);

    // Get all rows in hash key bucket 1 with range key >= 3.
    TableIterator<Integer, Integer, Integer> it = table.range(1, 3);

    // Prints 300, 200, and 100.
    while(it.hasNext()) {
      System.out.println(it.next().getValue());
    }

##### Versioned Values

Lastly, EZDB provides some utility functions to handle basic versioning of your data.

    RangeTable<Integer, String, Versioned<Integer>> table = ezdb.getTable("test-versioned-values", IntegerSerde.get, StringSerde.get, new VersionedSerde<Integer>(IntegerSerde.get));
    
    table.put(1213, "foo", new Versioned<Integer>(2, 0));
    table.put(1213, "foo", new Versioned<Integer>(3, 1));
    table.put(1213, new Versioned<Integer>(12345678, 0));
    
    // Prints obj=3, version=1.
    System.out.println(table.get(1213, "foo"))
    
    // Prints obj=12345678, version=0.
    System.out.println(table.get(1213))

##### TODO

* If we wish to use filtering to improve performance, we need to make sure that the bloom filter implementation LevelDB provides is compatible with our EzLevelDbCompartor and key byte format.
* Mavenize.
* Get the Javadocs up somewhere.
* Write a torture test.
