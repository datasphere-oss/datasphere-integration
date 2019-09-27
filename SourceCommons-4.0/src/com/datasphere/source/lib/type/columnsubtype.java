package com.datasphere.source.lib.type;

public enum columnsubtype
{
    WA_SIGNED_INTEGER(0), 
    WA_NORMAL_CHAR(1), 
    WA_NO_SUBTYPE(2);
    
    int type;
    
    private columnsubtype(final int type) {
        this.type = type;
    }
    
    public int getSubType() {
        return this.type;
    }
}
