package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.client.*;
import org.yetiz.utils.hbase.utils.CallbackTask;
import org.yetiz.utils.hbase.utils.ResultTask;
import org.yetiz.utils.hbase.utils.Task;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yeti on 16/4/14.
 */
public class HAsyncTable {

	private LinkedBlockingQueue<AsyncPackage> asyncQueue;

	public HAsyncTable(LinkedBlockingQueue<AsyncPackage> asyncQueue) {
		this.asyncQueue = asyncQueue;
	}

	public void get(Get get, ResultTask callback) {
		asyncQueue.offer(new AsyncPackage(get, callback));
	}

	public void append(Append append, ResultTask callback) {
		asyncQueue.offer(new AsyncPackage(append, callback));
	}

	public void increment(Increment increment, ResultTask callback) {
		asyncQueue.offer(new AsyncPackage(increment, callback));
	}

	public void put(Put put, CallbackTask callback) {
		asyncQueue.offer(new AsyncPackage(put, callback));
	}

	public void delete(Delete delete, CallbackTask callback) {
		asyncQueue.offer(new AsyncPackage(delete, callback));
	}

	public void batch(List<Row> rows, ResultTask task) {
		rows.parallelStream()
			.forEach(row -> asyncQueue.offer(new AsyncPackage(row, task)));
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
