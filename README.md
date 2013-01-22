# EZDB

EZDB provides a nice API to wrap LevelDB. Here are some examples.

##### Simple Key/Value

   Db ezdb = new EzLevelDb(new File("/tmp"));
   Table<Integer, Integer, Integer> table = ezdb.getTable("test", IntegerSerde.get, IntegerSerde.get, IntegerSerde.get);
   table.put(1, 1);
   System.out.println(table.get(1)); // prints 1

##### Hash/Range Combination

   Db ezdb = new EzLevelDb(new File("/tmp"));
   Table<Integer, String, Integer> table = ezdb.getTable("test", IntegerSerde.get, StringSerde.get, IntegerSerde.get);
   table.put(1213, "20120102-foo", 1234);
   table.put(1213, "20120102-bar", 5678);
   TableIterator<Integer, String, Integer> it = table.range(1213, "20120102", "20120103");
   while(it.hasNext()) {
     System.out.println(it.next().getValue()); // prints 1234 then 5678
   }
