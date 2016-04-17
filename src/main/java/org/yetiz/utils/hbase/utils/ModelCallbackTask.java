package org.yetiz.utils.hbase.utils;

import org.yetiz.utils.hbase.HTableModel;

/**
 * Created by yeti on 16/4/17.
 */
public interface ModelCallbackTask<P extends HTableModel> extends Task {

	void callback(P model);
}
