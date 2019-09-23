package com.datasphere.exception;

public class NoOperatorFoundException extends RuntimeException
{
    private static final long serialVersionUID = 7671189737451328327L;
    
    public NoOperatorFoundException() {
    }
    
    public NoOperatorFoundException(final String s) {
        super(s);
    }
    
    public NoOperatorFoundException(final String s, final Throwable t) {
        super(s, t);
    }
    
    public NoOperatorFoundException(final Throwable cause) {
        super(cause);
    }
    
    public NoOperatorFoundException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
