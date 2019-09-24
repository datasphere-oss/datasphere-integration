package com.datasphere.common.exc;

public class TargetShutdownException extends Exception
{
    private static final long serialVersionUID = -7596411329248881658L;
    
    public TargetShutdownException() {
        super("Target going to shutdown");
    }
    
    public TargetShutdownException(final String message) {
        super(message);
    }
    
    public TargetShutdownException(final Throwable cause) {
        super(cause);
    }
    
    public TargetShutdownException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public TargetShutdownException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
