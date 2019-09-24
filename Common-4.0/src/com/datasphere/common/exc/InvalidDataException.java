package com.datasphere.common.exc;

public class InvalidDataException extends Exception
{
    private static final long serialVersionUID = -1963390711417089291L;
    
    public InvalidDataException() {
        super("Input data is either malformed or has an incorrect format");
    }
    
    public InvalidDataException(final String message) {
        super(message);
    }
    
    public InvalidDataException(final Throwable cause) {
        super(cause);
    }
    
    public InvalidDataException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public InvalidDataException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writeableStackTrace) {
        super(message, cause, enableSuppression, writeableStackTrace);
    }
}
