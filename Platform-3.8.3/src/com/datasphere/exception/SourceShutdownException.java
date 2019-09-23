package com.datasphere.exception;

public class SourceShutdownException extends Exception
{
    private static final long serialVersionUID = -9007897871791754045L;
    
    public SourceShutdownException() {
        super("Source going to shutdown");
    }
    
    public SourceShutdownException(final String message) {
        super(message);
    }
    
    public SourceShutdownException(final Throwable cause) {
        super(cause);
    }
    
    public SourceShutdownException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public SourceShutdownException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
