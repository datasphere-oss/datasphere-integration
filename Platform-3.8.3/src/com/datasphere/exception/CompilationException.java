package com.datasphere.exception;

public class CompilationException extends FatalException
{
    private static final long serialVersionUID = 3843494709186515537L;
    private int errStart;
    private int errEnd;
    
    public CompilationException() {
        this.errStart = -1;
        this.errEnd = -1;
    }
    
    public CompilationException(final String message) {
        super(message);
        this.errStart = -1;
        this.errEnd = -1;
    }
    
    public CompilationException(final String message, final int msgStart, final int msgEnd) {
        super(message);
        this.errStart = -1;
        this.errEnd = -1;
        this.errStart = msgStart;
        this.errEnd = msgEnd;
    }
    
    public CompilationException(final Throwable cause) {
        super(cause);
        this.errStart = -1;
        this.errEnd = -1;
    }
    
    public CompilationException(final String message, final Throwable cause) {
        super(message, cause);
        this.errStart = -1;
        this.errEnd = -1;
    }
    
    public CompilationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errStart = -1;
        this.errEnd = -1;
    }
}
