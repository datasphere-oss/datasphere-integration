package com.datasphere.exception;

public class CacheNotRunningException extends Warning
{
    private static final long serialVersionUID = 545426163340497235L;
    
    public CacheNotRunningException(final String s) {
        super(s);
    }
    
    public CacheNotRunningException(final String s, final Throwable t) {
        super(s, t);
    }
}
