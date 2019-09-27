package com.datasphere.TypeHandlers;

class BlobHandler extends ByteArrayHandler
{
    public BlobHandler(final String targetType) {
        super(OracleTypeHandler.BLOB, targetType);
    }
    
    @Override
    public String toString(final Object value) {
        return "<BLOB Value>";
    }
}
