package com.datasphere.Exceptions;

public class UpdateTypeMapException extends Exception
{
    private static final long serialVersionUID = 1L;
    private int colIndex;
    private Class<?> valueType;
    
    public UpdateTypeMapException(final int colIndex, final Class<?> valueType) {
        this.colIndex = colIndex;
        this.valueType = valueType;
    }
    
    public void index(final int index) {
        this.colIndex = index;
    }
    
    public int index() {
        return this.colIndex;
    }
    
    public void valueType(final Class<?> valueType) {
        this.valueType = valueType;
    }
    
    public Class<?> valueType() {
        return this.valueType;
    }
}
