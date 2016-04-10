package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/3/25.
 */
public class YHBaseException extends RuntimeException {
	public YHBaseException() {
	}

	public YHBaseException(String message) {
		super(message);
	}

	public YHBaseException(Throwable cause) {
		super(cause);
	}
}
