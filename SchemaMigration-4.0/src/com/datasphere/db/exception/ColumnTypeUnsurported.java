package com.datasphere.db.exception;

public class ColumnTypeUnsurported extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ColumnTypeUnsurported() {
		super();
	}

	public ColumnTypeUnsurported(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ColumnTypeUnsurported(String message, Throwable cause) {
		super(message, cause);
	}

	public ColumnTypeUnsurported(String message) {
		super(message);
	}

	public ColumnTypeUnsurported(Throwable cause) {
		super(cause);
	}
}
