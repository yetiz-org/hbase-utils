package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/1.
 */
public class UnHandledException extends YHBaseException {
	public UnHandledException() {
	}

	public UnHandledException(String message) {
		super(message);
	}

	public UnHandledException(Throwable cause) {
		super(cause);
	}
}
