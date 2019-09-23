package com.datasphere.exception;

import com.esotericsoftware.kryonet.rmi.*;

public class RuntimeInterruptedException extends RuntimeException
{
    public RuntimeInterruptedException(final TimeoutException e) {
        super((Throwable)e);
    }
    
    public RuntimeInterruptedException(final Exception e) {
        super(e);
    }
}
