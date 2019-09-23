package com.datasphere.exception;

public class Warning extends RuntimeException
{
    private static final long serialVersionUID = -3680640083544369451L;
    
    public Warning() {
    }
    
    public Warning(final String s) {
        super(s);
    }
    
    public Warning(final String s, final Throwable t) {
        super(s, t);
    }
    
    public Warning(final Throwable cause) {
        super(cause);
    }
    
    public Warning(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
