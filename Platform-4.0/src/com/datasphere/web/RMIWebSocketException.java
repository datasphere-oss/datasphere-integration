package com.datasphere.web;

public class RMIWebSocketException extends Exception
{
    private static final long serialVersionUID = -855688986456900709L;
    
    public RMIWebSocketException(final String s) {
        super(s);
    }
    
    public RMIWebSocketException(final String s, final Throwable t) {
        super(s, t);
    }
}
