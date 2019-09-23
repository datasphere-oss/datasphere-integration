package com.datasphere.runtime.exceptions;

import java.io.*;

public class InvalidFormatException extends RuntimeException implements Serializable
{
    private static final long serialVersionUID = -4976794072771563490L;
    
    public InvalidFormatException() {
    }
    
    public InvalidFormatException(final String message) {
        super(message);
    }
    
    public InvalidFormatException(final Throwable cause) {
        super(cause);
    }
    
    public InvalidFormatException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public InvalidFormatException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
