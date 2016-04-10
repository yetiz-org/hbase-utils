package org.yetiz.utils.hbase.exception;

/**
 * Created by yeti on 16/4/1.
 */
public class CatcherRaiseException extends YHBaseException {
	public CatcherRaiseException() {
	}

	public CatcherRaiseException(String message) {
		super(message);
	}

	public CatcherRaiseException(Throwable cause) {
		super(cause);
	}
}
