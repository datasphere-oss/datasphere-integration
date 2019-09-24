package com.datasphere.common.errors;

public class RetriableException extends Exception
{
    public RetriableException() {
    }
    
    public RetriableException(final String message) {
        super(message);
    }
    
    public RetriableException(final Throwable cause) {
        super(cause);
    }
    
    public RetriableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
