package com.datasphere.Exceptions;

public class UnsupportedMappingException extends Exception
{
    public int targetType;
    public String sourceType;
    
    public UnsupportedMappingException(final int tgtType, final String srcType) {
        this.targetType = tgtType;
        this.sourceType = srcType;
    }
    
    public UnsupportedMappingException(final String exceptionMessage, final CloneNotSupportedException e) {
        super(exceptionMessage, e);
    }
}
