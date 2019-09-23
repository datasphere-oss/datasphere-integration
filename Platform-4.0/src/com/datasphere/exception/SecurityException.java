package com.datasphere.exception;

public class SecurityException extends Warning
{
    private static final long serialVersionUID = -7001586791561708133L;
    
    public SecurityException() {
    }
    
    public SecurityException(final String message) {
        super(message);
    }
    
    public SecurityException(final Throwable cause) {
        super(cause);
    }
    
    public SecurityException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public SecurityException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
