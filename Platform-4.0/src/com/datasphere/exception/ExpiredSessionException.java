package com.datasphere.exception;

public class ExpiredSessionException extends SecurityException
{
    private static final long serialVersionUID = -7001586791541788133L;
    
    public ExpiredSessionException() {
    }
    
    public ExpiredSessionException(final String message) {
        super(message);
    }
    
    public ExpiredSessionException(final Throwable cause) {
        super(cause);
    }
    
    public ExpiredSessionException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ExpiredSessionException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
