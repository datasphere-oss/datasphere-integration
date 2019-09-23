package com.datasphere.exception;

public class ServerException extends Exception
{
    private static final long serialVersionUID = 3846104744413605441L;
    
    public ServerException() {
    }
    
    public ServerException(final String message) {
        super(message);
    }
    
    public ServerException(final Throwable cause) {
        super(cause);
    }
    
    public ServerException(final String message, final Throwable cause) {
        super(message, cause);
    }
    
    public ServerException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
