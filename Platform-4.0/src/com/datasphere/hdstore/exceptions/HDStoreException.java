package com.datasphere.hdstore.exceptions;

public class HDStoreException extends RuntimeException
{
    private static final long serialVersionUID = 6924180541153454102L;
    
    public HDStoreException(final String message) {
        super(message);
    }
    
    public HDStoreException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
