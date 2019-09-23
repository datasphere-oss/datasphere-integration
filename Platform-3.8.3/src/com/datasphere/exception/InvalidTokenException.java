package com.datasphere.exception;

public class InvalidTokenException extends SecurityException
{
    public InvalidTokenException() {
    }
    
    public InvalidTokenException(final String message) {
        super(message);
    }
    
    public InvalidTokenException(final Throwable cause) {
        super(cause);
    }
    
    public InvalidTokenException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public InvalidTokenException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
