package com.datasphere.source.lib.type;

public enum columntype
{
    WA_UNSUPPORTED(0), 
    WA_BYTE(1), 
    WA_SIGNED_BYTE(2), 
    WA_SHORT(3), 
    WA_SIGNED_SHORT(4), 
    WA_INTEGER(5), 
    WA_SIGNED_INTEGER(6), 
    WA_LONG(7), 
    WA_SIGNED_LONG(8), 
    WA_FLOAT(9), 
    WA_DOUBLE(10), 
    WA_STRING(11), 
    WA_UTF16_STRING(12), 
    WA_BINARY(13), 
    WA_DATETIME(14), 
    WA_DATE(15), 
    WA_BLOB(16), 
    WA_CLOB(17), 
    WA_UTF16_CLOB(18);
    
    int type;
    
    private columntype(final int type) {
        this.type = type;
    }
    
    public int getType() {
        return this.type;
    }
}
