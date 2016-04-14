package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/1.
 */
public class NotSupportException extends YHBaseException {
	public NotSupportException() {
	}

	public NotSupportException(String message) {
		super(message);
	}

	public NotSupportException(Throwable cause) {
		super(cause);
	}
}
