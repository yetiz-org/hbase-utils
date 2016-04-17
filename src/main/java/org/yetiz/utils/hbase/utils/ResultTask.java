package org.yetiz.utils.hbase.utils;

import org.apache.hadoop.hbase.client.Result;

/**
 * Created by yeti on 16/4/17.
 */
public interface ResultTask extends Task {
	void callback(Result result);
}
