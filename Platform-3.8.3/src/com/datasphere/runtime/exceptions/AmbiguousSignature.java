package com.datasphere.runtime.exceptions;

public class AmbiguousSignature extends Exception
{
    private static final long serialVersionUID = 7612779530365368948L;
    
    public AmbiguousSignature() {
    }
    
    public AmbiguousSignature(final String message) {
        super(message);
    }
}
