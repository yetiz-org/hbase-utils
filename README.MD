![Maven Repository](https://maven-badges.herokuapp.com/maven-central/org.yetiz.utils/hbase-utils/badge.png)

# HBase-Utils

##  Project for HBase operation, define a DAO model and wrap HBase operation. Create a easy way to do batch operation and other admin command.

```java
    HBaseClient client = HBaseClient.Builder
                         			.create()
                         			.set(HBaseClient.Parameter.ZK_QUORUM, QuorumHost)
                         			.setLong(HBaseClient.Parameter.ZK_PROPERTY_CLIENT_PORT, QuorumPort)
                         			.build();
    client.admin().createTable(tableName);
    client.admin().addColumnFamily(tableName, "F", Compression.Algorithm.NONE);
    client.admin().updateCompression(tableName, "F", Compression.Algorithm.LZ4);
    HBaseTable table = client.table(tableName);
    table.put(new Put("ROW".getBytes()).addColumn("F".getBytes(), "FQ".getBytes(), "VAL".getBytes()));
    table.get(new Get("ROW".getBytes()));
	List<Row> asyncPuts = new ArrayList<>();
	List<Row> fastPuts = new ArrayList<>();
	for (int i = 0; i < 10; i++) {
		asyncPuts.add(new Put(("ASYNC" + i).getBytes()).addColumn("F".getBytes(), "FQ".getBytes(), "V".getBytes()));
		fastPuts.add(new Put(("FAST" + i).getBytes()).addColumn("F".getBytes(), "FQ".getBytes(), "V".getBytes()));
	}

	tableNames.parallelStream()
		.forEach(tableName -> {
			HBaseTable table = client.table(tableName);
			table.fast().go(fastPuts);
			table.async().batch(asyncPuts, result -> System.out.println("OK"));
			});

	client.table(HTableModel.tableName(Account.class))
		.model()
		.scan(new Scan("STARTKEY".getBytes()))
		.<Account>forEach(account -> System.out.println(account.username()));

    client.admin().deleteColumnFamily(tableName, "F");
    client.admin().truncateTable(tableName);
    client.admin().deleteTable(tableName);
```