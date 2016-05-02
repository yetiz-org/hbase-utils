package org.yetiz.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yetiz.utils.hbase.exception.DataSourceException;
import org.yetiz.utils.hbase.exception.UnHandledException;
import org.yetiz.utils.hbase.exception.YHBaseException;
import org.yetiz.utils.hbase.utils.CallbackTask;
import org.yetiz.utils.hbase.utils.ResultTask;
import org.yetiz.utils.hbase.utils.Task;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by yeti on 16/4/1.
 */
public final class HBaseClient {
	public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");
	private static final int DEFAULT_MAX_FAST_BATCH_COUNT = 5000;
	private static final int DEFAULT_MAX_ASYNC_BATCH_COUNT = 5000;
	private static final ExecutorService EXECUTOR =
		new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5L, TimeUnit.SECONDS, new SynchronousQueue<>());
	private static final AtomicLong INCREMENT_ID = new AtomicLong(0);
	protected final HashMap<TableName, LinkedBlockingQueue<Row>>
		fastCollection = new HashMap<>();
	protected final HashMap<TableName, LinkedBlockingQueue<HAsyncTable.AsyncPackage>>
		asyncCollection = new HashMap<>();
	private final boolean reproducible;
	private final String id = String.format("%s-%d", HBaseClient.class.getName(), INCREMENT_ID.getAndIncrement());
	private final Logger logger = LoggerFactory.getLogger(id);
	private volatile int fastBatchCount = DEFAULT_MAX_FAST_BATCH_COUNT - 1;
	private volatile int asyncBatchCount = DEFAULT_MAX_ASYNC_BATCH_COUNT - 1;
	private volatile boolean closed = false;
	private Connection connection;
	private Configuration configuration = HBaseConfiguration.create();

	private HBaseClient(boolean reproducible) {
		this.reproducible = reproducible;
	}

	public static final byte[] bytes(String string) {
		return string.getBytes(DEFAULT_CHARSET);
	}

	/**
	 * Only for Get, Put, Delete, Append, Increment.<br>
	 * With callback
	 *
	 * @return <code>Async</code>
	 */
	public HAsyncTable async(TableName tableName) {
		return new HAsyncTable(asyncQueue(tableName));
	}

	/**
	 * Only for Get, Put, Delete, Append, Increment.<br>
	 * No callback, this is faster then <code>async()</code>
	 *
	 * @return <code>Fast</code>
	 */
	public HFastTable fast(TableName tableName) {
		return new HFastTable(fastQueue(tableName));
	}

	public HFastTable fast(HTableModel model) {
		return new HFastTable(fastQueue(model.tableName()));
	}

	public int fastBatchCount() {
		return fastBatchCount;
	}

	public HBaseClient setFastBatchCount(int fastBatchCount) {
		this.fastBatchCount = fastBatchCount - 1;
		return this;
	}

	public int asyncBatchCount() {
		return asyncBatchCount;
	}

	public HBaseClient setAsyncBatchCount(int asyncBatchCount) {
		this.asyncBatchCount = asyncBatchCount - 1;
		return this;
	}

	private void init() {
		this.connection = newConnection();
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
		logger.debug("Close " + id());
		this.closed = true;
		try {
			EXECUTOR.shutdown();
			while (!EXECUTOR.awaitTermination(1, TimeUnit.SECONDS)) ;
		} catch (InterruptedException e) {
		}
		logger.debug(id() + " Closed");
	}

	private String id() {
		return id;
	}

	public boolean closed() {
		return this.closed;
	}

	private void fastLoopTask(TableName tableName, LinkedBlockingQueue<Row> fastQueue, boolean isMaster) {
		List<Row> rows = new ArrayList<>();

		while (!closed()) {
			try {
				Row row = fastQueue.poll(1, TimeUnit.SECONDS);
				if (row == null) {
					if (!isMaster) {
						break;
					}

					continue;
				}

				rows.add(row);
				if (fastQueue.drainTo(rows, fastBatchCount()) == fastBatchCount() && reproducible) {
					EXECUTOR.execute(() -> fastLoopTask(tableName, fastQueue, false));
				}

				Object[] results = new Object[rows.size()];
				HBaseTable table = null;
				try {
					table = table(tableName);
					table.batch(rows, results);
				} catch (Throwable throwable) {
					throw convertedException(throwable);
				} finally {
					if (table != null) {
						table.close();
					}
				}
			} catch (Throwable throwable) {
			}

			if (!isMaster) {
				break;
			}
			rows = new ArrayList<>();
		}
	}

	protected LinkedBlockingQueue<Row> fastQueue(TableName tableName) {
		if (!fastCollection.containsKey(tableName)) {
			synchronized (fastCollection) {
				if (!fastCollection.containsKey(tableName)) {
					LinkedBlockingQueue<Row> fastQueue = new LinkedBlockingQueue<>();
					fastCollection.put(tableName, fastQueue);
					EXECUTOR.execute(() -> fastLoopTask(tableName, fastQueue, true));
				}
			}
		}

		return fastCollection.get(tableName);
	}

	private void asyncLoopTask(TableName tableName,
	                           LinkedBlockingQueue<HAsyncTable.AsyncPackage> asyncQueue,
	                           boolean isMaster) {
		List<HAsyncTable.AsyncPackage> packages = new ArrayList<>();

		while (!closed()) {
			try {
				HAsyncTable.AsyncPackage aPackage = asyncQueue.poll(1, TimeUnit.SECONDS);
				if (aPackage == null) {
					if (!isMaster) {
						break;
					}

					continue;
				}

				packages.add(aPackage);
				if (asyncQueue.drainTo(packages, asyncBatchCount()) == asyncBatchCount() && reproducible) {
					EXECUTOR.execute(() -> asyncLoopTask(tableName, asyncQueue, false));
				}

				List<Row> rows = new ArrayList<>();
				packages.stream()
					.forEach(asyncPackage -> rows.add(asyncPackage.action));

				Object[] results = new Object[packages.size()];
				HAsyncTable.AsyncPackage[] packageArray = new HAsyncTable.AsyncPackage[packages.size()];
				packages.toArray(packageArray);

				HBaseTable table = null;
				try {
					table = table(tableName);
					table.batch(rows, results);
				} catch (Throwable throwable) {
					throw convertedException(throwable);
				} finally {
					if (table != null) {
						table.close();
					}
				}

				HashMap<HAsyncTable.AsyncPackage, Object> invokes = new HashMap<>();
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
			} catch (Throwable throwable) {
			}

			if (!isMaster) {
				break;
			}
		}
	}

	protected LinkedBlockingQueue<HAsyncTable.AsyncPackage> asyncQueue(TableName tableName) {
		if (!asyncCollection.containsKey(tableName)) {
			synchronized (asyncCollection) {
				if (!asyncCollection.containsKey(tableName)) {
					LinkedBlockingQueue<HAsyncTable.AsyncPackage> asyncQueue = new LinkedBlockingQueue<>();
					asyncCollection.put(tableName, asyncQueue);
					EXECUTOR.execute(() -> asyncLoopTask(tableName, asyncQueue, true));
				}
			}
		}

		return asyncCollection.get(tableName);
	}

	public HBaseTable table(HTableModel model) {
		return table(model.tableName());
	}

	public HBaseTable table(TableName tableName) {
		try {
			return new HBaseTable(tableName,
				connection().getTable(tableName.get()));
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	protected Connection connection() {
		return connection;
	}

	private YHBaseException convertedException(Throwable throwable) {
		if (throwable instanceof YHBaseException) {
			return (YHBaseException) throwable;
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
			return create(true);
		}

		/**
		 * @param reproducible reproduce worker when fast or async worker is busy.
		 * @return
		 */
		public static final Builder create(boolean reproducible) {
			Builder builder = new Builder();
			builder.hBaseClient = new HBaseClient(reproducible);
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
