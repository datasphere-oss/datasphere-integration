package com.datasphere.runtime.exceptions;

public class SignatureNotFound extends Exception
{
    private static final long serialVersionUID = 4606993564528798750L;
    
    public SignatureNotFound() {
    }
    
    public SignatureNotFound(final String message) {
        super(message);
    }
}
