package com.datasphere.Exceptions;

public enum Error
{
    MAPPED_COLUMN_DOES_NOT_EXISTS(2754, "Mapped Column does not exists."), 
    INCONSISTENT_TABLE_STRUCTURE(2751, "Inconsistent source & target table strcture");
    
    private int type;
    private String text;
    
    private Error(final int type, final String text) {
        this.type = type;
        this.text = text;
    }
    
    @Override
    public String toString() {
        return this.type + " : " + this.text;
    }
    
    public int getType() {
        return this.type;
    }
}
