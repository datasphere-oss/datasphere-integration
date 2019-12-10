package com.datasphere.source.lib.type;

public enum columnsubtype
{
    HD_SIGNED_INTEGER(0), 
    HD_NORMAL_CHAR(1), 
    HD_NO_SUBTYPE(2);
    
    int type;
    
    private columnsubtype(final int type) {
        this.type = type;
    }
    
    public int getSubType() {
        return this.type;
    }
}
