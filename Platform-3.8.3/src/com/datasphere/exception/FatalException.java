package com.datasphere.exception;

public class FatalException extends RuntimeException
{
    private static final long serialVersionUID = 7671189737451328327L;
    
    public FatalException() {
    }
    
    public FatalException(final String s) {
        super(s);
    }
    
    public FatalException(final String s, final Throwable t) {
        super(s, t);
    }
    
    public FatalException(final Throwable cause) {
        super(cause);
    }
    
    public FatalException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
