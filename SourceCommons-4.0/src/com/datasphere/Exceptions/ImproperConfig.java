package com.datasphere.Exceptions;

public class ImproperConfig extends Exception
{
    private static final long serialVersionUID = 1L;
    String errorMsg;
    
    public ImproperConfig(final String err) {
        this.errorMsg = err;
    }
    
    @Override
    public String getMessage() {
        return this.errorMsg;
    }
}
