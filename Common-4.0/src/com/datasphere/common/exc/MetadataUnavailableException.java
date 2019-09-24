package com.datasphere.common.exc;

import java.io.*;

public class MetadataUnavailableException extends SystemException implements Serializable
{
    private static final long serialVersionUID = 1671729232291071244L;
    
    public MetadataUnavailableException() {
        super("Resource not available. Source is going to Shutdown");
    }
    
    public MetadataUnavailableException(final String message) {
        super(message);
    }
    
    public MetadataUnavailableException(final Throwable cause) {
        super(cause);
    }
    
    public MetadataUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public MetadataUnavailableException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
