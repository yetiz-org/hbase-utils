package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/9.
 */
public class InvalidOperationException extends YHBaseException {
	public InvalidOperationException() {
	}

	public InvalidOperationException(String message) {
		super(message);
	}

	public InvalidOperationException(Throwable cause) {
		super(cause);
	}
}
