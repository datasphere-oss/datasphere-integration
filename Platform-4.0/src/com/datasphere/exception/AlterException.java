package com.datasphere.exception;

public class AlterException extends FatalException
{
    public AlterException() {
    }
    
    public AlterException(final String s) {
        super(s);
    }
    
    public AlterException(final String s, final Throwable t) {
        super(s, t);
    }
    
    public AlterException(final Throwable cause) {
        super(cause);
    }
    
    public AlterException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
