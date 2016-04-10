package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/1.
 */
public class DataSourceException extends YHBaseException {
	public DataSourceException() {
	}

	public DataSourceException(String message) {
		super(message);
	}

	public DataSourceException(Throwable cause) {
		super(cause);
	}
}
