package com.datasphere.exception;

public class OperationNotSupported extends Exception
{
    public String operation;
    
    public OperationNotSupported(final String operation) {
        this.operation = operation;
    }
    
    @Override
    public String toString() {
        return "Operation {" + this.operation + "} is not supported";
    }
}
