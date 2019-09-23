package com.datasphere.exception;

public class InvalidUriException extends Warning
{
    public InvalidUriException(final String s) {
        super(s);
    }
    
    public InvalidUriException(final String s, final Throwable t) {
        super(s, t);
    }
}
