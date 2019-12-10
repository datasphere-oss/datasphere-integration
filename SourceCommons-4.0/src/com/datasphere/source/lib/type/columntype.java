package com.datasphere.source.lib.type;

public enum columntype
{
    HD_UNSUPPORTED(0), 
    HD_BYTE(1), 
    HD_SIGNED_BYTE(2), 
    HD_SHORT(3), 
    HD_SIGNED_SHORT(4), 
    HD_INTEGER(5), 
    HD_SIGNED_INTEGER(6), 
    HD_LONG(7), 
    HD_SIGNED_LONG(8), 
    HD_FLOAT(9), 
    HD_DOUBLE(10), 
    HD_STRING(11), 
    HD_UTF16_STRING(12), 
    HD_BINARY(13), 
    HD_DATETIME(14), 
    HD_DATE(15), 
    HD_BLOB(16), 
    HD_CLOB(17), 
    HD_UTF16_CLOB(18);
    
    int type;
    
    private columntype(final int type) {
        this.type = type;
    }
    
    public int getType() {
        return this.type;
    }
}
