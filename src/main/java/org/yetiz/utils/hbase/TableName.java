package org.yetiz.utils.hbase;

import java.util.HashMap;

/**
 * Created by yeti on 16/4/2.
 */
public final class TableName {
	private static final HashMap<String, TableName> maps = new HashMap<>();
	private org.apache.hadoop.hbase.TableName tableName;

	private TableName() {
	}

	public static final TableName valueOf(String tableName) {
		TableName rtn = maps.get(tableName);
		if (rtn == null) {
			synchronized (maps) {
				if (rtn == null) {
					rtn = new TableName();
					rtn.tableName = org.apache.hadoop.hbase.TableName.valueOf(tableName);
					maps.put(tableName, rtn);
				}
			}
		}

		return rtn;
	}

	public final org.apache.hadoop.hbase.TableName get() {
		return tableName;
	}
}
