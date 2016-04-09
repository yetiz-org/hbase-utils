package org.yetiz.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.slf4j.LoggerFactory;
import org.yetiz.utils.exception.DataSourceException;
import org.yetiz.utils.exception.UnHandledException;
import org.yetiz.utils.exception.YException;
import org.yetiz.utils.hbase.HBaseTable.Async;
import org.yetiz.utils.hbase.HBaseTable.CallbackTask;
import org.yetiz.utils.hbase.HBaseTable.ResultTask;
import org.yetiz.utils.hbase.HBaseTable.Task;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yeti on 16/4/1.
 */
public class HBaseClient {
	public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");
	public static final int DEFAULT_INVOKER_COUNT = 1;
	private static final int MAX_FAST_BATCH_SIZE = 5000;
	private static final int MAX_ASYNC_BATCH_SIZE = 5000;
	protected final ConcurrentHashMap<TableName, LinkedBlockingQueue<Row>>
		fastCollection = new ConcurrentHashMap<>();
	protected final ConcurrentHashMap<TableName, LinkedBlockingQueue<Async.AsyncPackage>>
		asyncPackages = new ConcurrentHashMap<>();
	private final int invokerCount;
	private boolean closed = false;
	private Connection connection;
	private Configuration configuration = HBaseConfiguration.create();

	private HBaseClient(int invokerCount) {
		this.invokerCount = invokerCount;
	}

	public static final byte[] bytes(String string) {
		return string.getBytes(DEFAULT_CHARSET);
	}

	private void init() {
		this.connection = newConnection();
		for (int i = 0; i < invokerCount; i++) {
			new MiniConsumer(this).start();
		}
	}

	private Connection newConnection() {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			return connection;
		} catch (Exception e) {
			throw new DataSourceException(e);
		}
	}

	public HBaseAdmin admin() {
		try {
			return new HBaseAdmin(connection.getAdmin());
		} catch (Exception e) {
			throw new DataSourceException(e);
		}
	}

	public void close() {
		this.closed = true;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public HBaseTable table(TableName tableName) {
		try {
			return new HBaseTable(tableName,
				connection().getTable(tableName.get()),
				asyncPackages,
				fastCollection);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	protected Connection connection() {
		return connection;
	}

	private YException convertedException(Throwable throwable) {
		if (throwable instanceof YException) {
			return (YException) throwable;
		} else {
			return new UnHandledException(throwable);
		}
	}

	public long rowCount(TableName tableName, byte[] family) {
		AggregationClient aggregationClient = new AggregationClient(configuration());
		Scan scan = new Scan();
		scan.addFamily(family);
		long count = 0;
		try {
			count = aggregationClient.rowCount(tableName.get(), null, scan);
		} catch (Throwable e) {
		}
		return count;
	}

	public Configuration configuration() {
		return configuration;
	}

	public static class MiniConsumer extends Thread {
		private HBaseClient client;

		public MiniConsumer(HBaseClient client) {
			this.client = client;
		}

		@Override
		public void run() {
			while (!client.isClosed()) {
				try {
					drainFast();
				} catch (Throwable throwable) {
					LoggerFactory.getLogger(MiniConsumer.class).error("drainFast exception occur.", throwable);
				}

				try {
					drainAsync();
					Thread.sleep(100);
				} catch (InterruptedException e) {
				} catch (Throwable throwable) {
					LoggerFactory.getLogger(MiniConsumer.class).error("drainAsync exception occur.", throwable);
				}
			}
		}

		private void drainFast() {
			client.fastCollection
				.entrySet()
				.parallelStream()
				.forEach(pair -> {
					List<Row> rows = new ArrayList<>();
					pair.getValue().drainTo(rows, MAX_FAST_BATCH_SIZE);
					if (rows.isEmpty()) {
						return;
					}

					Object[] results = new Object[rows.size()];
					Connection connection = null;
					try {
						client.table(pair.getKey()).batch(rows, results);
					} catch (Throwable throwable) {
						throw client.convertedException(throwable);
					} finally {
						try {
							connection.close();
						} catch (Throwable throwable) {
						}
					}
				});
		}

		private void drainAsync() {
			client.asyncPackages
				.entrySet()
				.parallelStream()
				.forEach(pair -> {
					List<Async.AsyncPackage> packages = new ArrayList<>();
					List<Row> rows = new ArrayList<>();
					pair.getValue().drainTo(packages, MAX_ASYNC_BATCH_SIZE);
					if (packages.isEmpty()) {
						return;
					}

					packages.stream()
						.forEach(asyncPackage -> rows.add(asyncPackage.action));

					Object[] results = new Object[packages.size()];
					Async.AsyncPackage[] packageArray = new Async.AsyncPackage[packages.size()];
					packages.toArray(packageArray);

					Connection connection = null;
					try {
						connection = client.newConnection();
						client.table(pair.getKey()).batch(rows, results);
					} catch (Throwable throwable) {
						throw client.convertedException(throwable);
					} finally {
						try {
							connection.close();
						} catch (Throwable throwable) {
						}
					}

					HashMap<Async.AsyncPackage, Object> invokes = new HashMap<>();
					for (int i = 0; i < results.length; i++) {
						invokes.put(packageArray[i], results[i]);
					}

					invokes.entrySet()
						.parallelStream()
						.forEach(entry -> {
							Task task = entry.getKey().callback;
							if (task == null) {
								return;
							}

							if (task instanceof ResultTask) {
								((ResultTask) task).callback(((Result) entry.getValue()));
							}

							if (task instanceof CallbackTask) {
								((CallbackTask) task).callback();
							}
						});
				});
		}
	}

	public static class Parameter {
		public static final Parameter ZK_QUORUM =
			new Parameter(HConstants.ZOOKEEPER_QUORUM);
		public static final Parameter ZK_PROPERTY_CLIENT_PORT =
			new Parameter(HConstants.ZOOKEEPER_CLIENT_PORT);
		public static final Parameter CLIENT_SCANNER_CACHING =
			new Parameter(HConstants.HBASE_CLIENT_SCANNER_CACHING);
		public static final Parameter RPC_TIMEOUT =
			new Parameter(HConstants.HBASE_RPC_TIMEOUT_KEY);
		public static final Parameter ZK_SESSION_TIMEOUT =
			new Parameter(HConstants.ZK_SESSION_TIMEOUT);
		private String name;

		private Parameter(String name) {
			this.name = name;
		}

		public String name() {
			return name;
		}
	}

	public static class Builder {
		private HBaseClient hBaseClient;

		public static final Builder create() {
			return create(DEFAULT_INVOKER_COUNT);
		}

		public static final Builder create(int invokerCount) {
			Builder builder = new Builder();
			builder.hBaseClient = new HBaseClient(invokerCount);
			return builder;
		}

		public final Builder configuration(Configuration configuration) {
			hBaseClient.configuration = configuration;
			return this;
		}

		public final Builder set(Parameter key, String value) {
			hBaseClient.configuration.set(key.name(), value);
			return this;
		}

		public final Builder setLong(Parameter key, Long value) {
			hBaseClient.configuration.setLong(key.name(), value);
			return this;
		}

		public final HBaseClient build() {
			hBaseClient.init();
			return hBaseClient;
		}
	}

}
