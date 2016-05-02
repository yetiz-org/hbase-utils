package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.yetiz.utils.hbase.exception.UnHandledException;
import org.yetiz.utils.hbase.exception.YHBaseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by yeti on 16/4/5.
 */
public class HBaseTable {
	private final Table table;
	private final TableName tableName;
	private final Model model;
	private boolean closed = false;

	protected HBaseTable(TableName tableName,
	                     Table table) {
		this.tableName = tableName;
		this.table = table;
		this.model = new Model(table, tableName);
	}

	public <R extends HTableModel> Model<R> model() {
		return model;
	}

	public void close() {
		try {
			table().close();
		} catch (Throwable throwable) {
			throw convertedException(throwable);
		} finally {
			closed = true;
		}
	}

	private Table table() {
		return table;
	}

	private YHBaseException convertedException(Throwable throwable) {
		if (throwable instanceof YHBaseException) {
			return (YHBaseException) throwable;
		} else {
			return new UnHandledException(throwable);
		}
	}

	public boolean isClosed() {
		return closed;
	}

	public boolean exists(Get get) {
		try {
			return table().exists(get);
		} catch (Throwable throwable) {
			throw convertedException(throwable);
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

	public static class Model<R extends HTableModel> {
		private Table table;
		private TableName tableName;

		public Model(Table table, TableName tableName) {
			this.table = table;
			this.tableName = tableName;
		}

		public R append(Append append) {
			try {
				return convert(table.append(append));
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

		private R convert(Result result) {
			return HTableModel.newWrappedModel(tableName, result);
		}

		public R increment(Increment increment) {
			try {
				return convert(table.increment(increment));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public R get(Get get) {
			try {
				return convert(table.get(get));
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public List<R> get(List<Get> gets) {
			try {
				return Arrays.asList(table.get(gets))
					.stream()
					.collect(ArrayList::new,
						(list, get) -> list.add(convert(get)),
						(list1, list2) -> list1.addAll(list2));

			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public ReturnScanner<R> scan(Scan scan) {
			try {
				return new ReturnScanner<>(table.getScanner(scan), tableName);
			} catch (Throwable throwable) {
				throw convertedException(throwable);
			}
		}

		public static class ReturnScanner<R extends HTableModel> {
			private ResultScanner scanner;
			private TableName tableName;

			public ReturnScanner(ResultScanner scanner, TableName tableName) {
				this.scanner = scanner;
				this.tableName = tableName;
			}

			public R next() throws IOException {
				return convert(scanner.next());
			}

			private R convert(Result result) {
				return HTableModel.newWrappedModel(tableName, result);
			}

			public List<R> next(int nbRows) throws IOException {
				return Arrays.asList(scanner.next(nbRows))
					.stream()
					.collect(ArrayList::new,
						(list, get) -> list.add(convert(get)),
						(list1, list2) -> list1.addAll(list2));
			}

			public void close() {
				scanner.close();
			}

			public void forEach(Consumer<? super R> action) {
				scanner.forEach(result -> action.accept(this.convert(result)));
			}
		}
	}
}
