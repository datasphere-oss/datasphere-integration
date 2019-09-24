package com.datasphere.common.exc;

import java.io.*;

public class ConnectionException extends SystemException implements Serializable
{
    private static final long serialVersionUID = -868743265768367780L;
    
    public ConnectionException() {
        super("Resource not available. Source is going to Shutdown");
    }
    
    public ConnectionException(final String message) {
        super(message);
    }
    
    public ConnectionException(final Throwable cause) {
        super(cause);
    }
    
    public ConnectionException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ConnectionException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
