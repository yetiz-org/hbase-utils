package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.client.Row;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yeti on 16/4/14.
 */
public class HFastTable {
	private LinkedBlockingQueue<Row> fastQueue;

	public HFastTable(LinkedBlockingQueue<Row> fastQueue) {
		this.fastQueue = fastQueue;
	}

	public void go(Row action) {
		fastQueue.offer(action);
	}

	public void go(List<Row> actions) {
		fastQueue.addAll(actions);
	}
}
