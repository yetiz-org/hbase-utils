package org.yetiz.utils.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.yetiz.utils.hbase.exception.CatcherRaiseException;
import org.yetiz.utils.hbase.exception.DuplicateException;
import org.yetiz.utils.hbase.exception.UnHandledException;
import org.yetiz.utils.hbase.exception.YHBaseException;

/**
 * Created by yeti on 16/4/1.
 */
public class HBaseAdmin {
	private Admin admin;

	protected HBaseAdmin(Admin admin) {
		this.admin = admin;
	}

	public boolean tableExists(TableName tableName) {
		try {
			checkTableNotExist(tableName);
			return false;
		} catch (Throwable throwable) {
			return true;
		}
	}

	private void checkTableNotExist(TableName tableName) {
		CATCHER(() -> {
			try {
				if (admin().tableExists(tableName.get())) {
					throw new DuplicateException("table name existed");
				}
			} catch (Throwable throwable) {
				throw new UnHandledException(throwable);
			}
		});
	}

	private Admin admin() {
		return admin;
	}

	private final void CATCHER(Runnable task) {
		try {
			task.run();
		} catch (YHBaseException d) {
			throw d;
		} catch (Throwable t) {
			throw new CatcherRaiseException(t);
		}
	}

	public HTableDescriptor tableDescriptor(TableName tableName) {
		try {
			return admin().getTableDescriptor(tableName.get());
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void updateTable(TableName tableName, HTableDescriptor tableDescriptor) {
		try {
			admin().modifyTable(tableName.get(), tableDescriptor);
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public HTableDescriptor[] listTables() {
		try {
			return admin().listTables();
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void enableTable(TableName tableName) {
		if (!isTableDisabled(tableName)) {
			return;
		}

		try {
			admin().enableTable(tableName.get());
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public boolean isTableDisabled(TableName tableName) {
		try {
			return admin().isTableDisabled(tableName.get());
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void truncateTable(TableName tableName) {
		truncateTable(tableName, false);
	}

	public void truncateTable(TableName tableName, boolean preserveSplit) {
		disableTable(tableName);
		try {
			admin().truncateTable(tableName.get(), preserveSplit);
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void disableTable(TableName tableName) {
		if (!isTableDisabled(tableName)) {
			try {
				admin().disableTable(tableName.get());
			} catch (Throwable throwable) {
				throw new UnHandledException(throwable);
			}
		}
	}

	public void deleteTable(TableName tableName) {
		disableTable(tableName);
		try {
			admin().deleteTable(tableName.get());
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void updateCompression(TableName tableName, String family, Algorithm compression) {
		try {
			admin().modifyColumn(tableName.get(), new HColumnDescriptor(family).setCompressionType(compression));
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void addColumnFamily(TableName tableName, String family, Algorithm compression) {
		try {
			admin().addColumn(tableName.get(), new HColumnDescriptor(family).setCompressionType(compression));
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void deleteColumnFamily(TableName tableName, String family) {
		try {
			admin().deleteColumn(tableName.get(), HBaseClient.bytes(family));
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void majorCompact(TableName tableName) {
		majorCompact(tableName, null);
	}

	public void majorCompact(TableName tableName, String family) {
		try {
			admin().majorCompact(tableName.get(), family == null ? null : HBaseClient.bytes(family));
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void split(TableName tableName, byte[] splitPoint) {
		try {
			admin().split(tableName.get(), splitPoint);
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public boolean balancer() {
		try {
			return admin().balancer();
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void createTable(TableName tableName) {
		checkTableNotExist(tableName);
		try {
			admin().createTable(newTableDescriptor(tableName));
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	private HTableDescriptor newTableDescriptor(TableName tableName) {
		try {
			return new HTableDescriptor(tableName.get());
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}

	public void createTable(TableName tableName, byte[] startKey, byte[] endKey, int numberOfRegions) {
		checkTableNotExist(tableName);

		try {
			admin().createTable(newTableDescriptor(tableName), startKey, endKey, numberOfRegions);
		} catch (Throwable throwable) {
			throw new UnHandledException(throwable);
		}
	}
}
