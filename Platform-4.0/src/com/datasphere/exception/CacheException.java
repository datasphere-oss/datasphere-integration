package com.datasphere.exception;

public class CacheException extends Warning
{
    private static final long serialVersionUID = 545426163340497235L;
    
    public CacheException(final String s) {
        super(s);
    }
    
    public CacheException(final String s, final Throwable t) {
        super(s, t);
    }
}
