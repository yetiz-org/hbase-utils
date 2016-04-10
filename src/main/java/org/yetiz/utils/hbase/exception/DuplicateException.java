package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/1.
 */
public class DuplicateException extends YHBaseException {
	public DuplicateException() {
	}

	public DuplicateException(String message) {
		super(message);
	}

	public DuplicateException(Throwable cause) {
		super(cause);
	}
}
