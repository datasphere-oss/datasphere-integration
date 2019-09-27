package com.datasphere.source.lib.intf;

public interface IDDLRecord
{
    void setTimestamp(final long p0);
    
    void setPosition(final byte[] p0);
    
    void setOperationName(final String p0);
    
    void setCommand(final String p0);
    
    void setCatalogObjectName(final String p0);
    
    void setCatalogObjectType(final String p0);
}
