package com.datasphere.pool;

import java.io.*;

public class ResourceException extends Exception implements Serializable
{
    private static final long serialVersionUID = -39172957670536337L;
    
    public ResourceException() {
        super("Resource not available. Source is going to Shutdown");
    }
    
    public ResourceException(final String message) {
        super(message);
    }
    
    public ResourceException(final Throwable cause) {
        super(cause);
    }
    
    public ResourceException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ResourceException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
