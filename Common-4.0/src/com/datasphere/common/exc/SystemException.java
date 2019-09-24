package com.datasphere.common.exc;

import java.io.*;

public class SystemException extends Exception implements Serializable
{
    private static final long serialVersionUID = 1383389949889394868L;
    
    public SystemException() {
        super("Resource not available. Source is going to Shutdown");
    }
    
    public SystemException(final String message) {
        super(message);
    }
    
    public SystemException(final Throwable cause) {
        super(cause);
    }
    
    public SystemException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public SystemException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
