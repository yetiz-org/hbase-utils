package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.yetiz.utils.hbase.exception.UnHandledException;
import org.yetiz.utils.hbase.exception.YHBaseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Created by yeti on 16/4/5.
 */
public class HBaseTable {
	private final Table table;
	private final TableName tableName;
	private final Async async;
	private final Fast fast;
	private final Model model;

	protected HBaseTable(TableName tableName,
	                     Table table,
	                     ConcurrentHashMap<TableName, LinkedBlockingQueue<Async.AsyncPackage>> asyncPackages,
	                     ConcurrentHashMap<TableName, LinkedBlockingQueue<Row>> fastCollection) {
		this.tableName = tableName;
		this.table = table;
		this.async = new Async(tableName, asyncPackages);
		this.fast = new Fast(tableName, fastCollection);
		this.model = new Model();
	}

	public Model model() {
		return model;
	}

	public void release() {
		try {
			table().close();
		} catch (IOException e) {
		}
	}

	private Table table() {
		return table;
	}

	public boolean exists(Get get) {
		try {
			return table().exists(get);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	private YHBaseException convertedException(Throwable throwable) {
		if (throwable instanceof YHBaseException) {
			return (YHBaseException) throwable;
		} else {
			return new UnHandledException(throwable);
		}
	}

	public boolean[] exists(List<Get> gets) {
		try {
			return table().existsAll(gets);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public Result append(Append append) {
		try {
			return table().append(append);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public TableName tableName() {
		return tableName;
	}

	public Result increment(Increment increment) {
		try {
			return table().increment(increment);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public Result get(Get get) {
		try {
			return table().get(get);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public Result[] get(List<Get> gets) {
		try {
			return table().get(gets);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public void put(Put put) {
		try {
			table().put(put);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public void put(List<Put> puts) {
		try {
			table().put(puts);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public void delete(Delete delete) {
		try {
			table().delete(delete);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public void delete(List<Delete> deletes) {
		try {
			table().delete(deletes);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public ResultScanner scan(Scan scan) {
		try {
			return table().getScanner(scan);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public void batch(List<? extends Row> actions, Object[] results) {
		try {
			table().batch(actions, results);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	public <R> void batchCallback(List<? extends Row> actions,
	                              Object[] results,
	                              Batch.Callback<R> callback) {
		try {
			table().batchCallback(actions, results, callback);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		}
	}

	/**
	 * Only for Get, Put, Delete, Append, Increment.<br>
	 * With callback
	 *
	 * @return <code>Async</code>
	 */
	public Async async() {
		return async;
	}

	/**
	 * Only for Get, Put, Delete, Append, Increment.<br>
	 * No callback, this is faster then <code>async()</code>
	 *
	 * @return <code>Fast</code>
	 */
	public Fast fast() {
		return fast;
	}


	public interface Task {
	}

	public interface ResultTask extends Task {
		void callback(Result result);
	}

	public interface CallbackTask extends Task {
		void callback();
	}

	public static class Fast {
		private TableName tableName;
		private ConcurrentHashMap<TableName, LinkedBlockingQueue<Row>> fastCollection;

		public Fast(TableName tableName,
		            ConcurrentHashMap<TableName, LinkedBlockingQueue<Row>> fastCollection) {
			this.tableName = tableName;
			this.fastCollection = fastCollection;
		}

		public void go(Row action) {
			rows(tableName).offer(action);
		}

		private LinkedBlockingQueue<Row> rows(TableName tableName) {
			if (!fastCollection.containsKey(tableName)) {
				fastCollection.put(tableName, new LinkedBlockingQueue<>());
			}

			return fastCollection.get(tableName);
		}

		public void go(List<Row> actions) {
			rows(tableName).addAll(actions);
		}
	}

	public static class Async {

		private TableName tableName;
		private ConcurrentHashMap<TableName, LinkedBlockingQueue<AsyncPackage>> asyncPackages;

		public Async(TableName tableName,
		             ConcurrentHashMap<TableName, LinkedBlockingQueue<AsyncPackage>> asyncPackages) {
			this.tableName = tableName;
			this.asyncPackages = asyncPackages;
		}

		public void get(Get get, ResultTask callback) {
			packages(tableName).offer(new AsyncPackage(get, callback));
		}

		private LinkedBlockingQueue<AsyncPackage> packages(TableName tableName) {
			if (!asyncPackages.containsKey(tableName)) {
				asyncPackages.put(tableName, new LinkedBlockingQueue<>());
			}

			return asyncPackages.get(tableName);
		}

		public void append(Append append, ResultTask callback) {
			packages(tableName).offer(new AsyncPackage(append, callback));
		}

		public void increment(Increment increment, ResultTask callback) {
			packages(tableName).offer(new AsyncPackage(increment, callback));
		}

		public void put(Put put, CallbackTask callback) {
			packages(tableName).offer(new AsyncPackage(put, callback));
		}

		public void delete(Delete delete, CallbackTask callback) {
			packages(tableName).offer(new AsyncPackage(delete, callback));
		}

		public void batch(List<Row> rows, ResultTask task) {
			rows.parallelStream()
				.forEach(row -> packages(tableName).offer(new AsyncPackage(row, task)));
		}

		public class AsyncPackage {
			protected Row action;
			protected Task callback;

			public AsyncPackage(Row action, Task callback) {
				this.action = action;
				this.callback = callback;
			}
		}
	}

	public class Model {

		public <R extends HTableModel> R append(Append append) {
			try {
				return convert(table().append(append));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		private <R extends HTableModel> R convert(Result result) {
			return HTableModel.newWrappedModel(tableName, result);
		}

		public <R extends HTableModel> R increment(Increment increment) {
			try {
				return convert(table().increment(increment));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public <R extends HTableModel> R get(Get get) {
			try {
				return convert(table().get(get));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public <R extends HTableModel> List<R> get(List<Get> gets) {
			try {
				return Arrays.asList(table().get(gets))
					.stream()
					.collect(ArrayList::new,
						(list, get) -> list.add(convert(get)),
						(list1, list2) -> list1.addAll(list2));

			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public ReturnScanner scan(Scan scan) {
			try {
				return new ReturnScanner(table().getScanner(scan));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public class ReturnScanner {
			private ResultScanner scanner;

			public ReturnScanner(ResultScanner scanner) {
				this.scanner = scanner;
			}

			public <R extends HTableModel> R next() throws IOException {
				return convert(scanner.next());
			}

			private <R extends HTableModel> R convert(Result result) {
				return HTableModel.newWrappedModel(tableName, result);
			}

			public <R extends HTableModel> List<R> next(int nbRows) throws IOException {
				return Arrays.asList(scanner.next(nbRows))
					.stream()
					.collect(ArrayList::new,
						(list, get) -> list.add(convert(get)),
						(list1, list2) -> list1.addAll(list2));
			}

			public void close() {
				scanner.close();
			}

			public <R extends HTableModel> void forEach(Consumer<? super R> action) {
				scanner.forEach(result -> action.accept(this.<R>convert(result)));
			}
		}
	}
}
