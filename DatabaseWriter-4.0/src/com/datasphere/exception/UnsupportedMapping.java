package com.datasphere.exception;

public class UnsupportedMapping extends Exception
{
    public int targetType;
    public String sourceType;
    
    public UnsupportedMapping(final int tgtType, final String srcType) {
        this.targetType = tgtType;
        this.sourceType = srcType;
    }
}
