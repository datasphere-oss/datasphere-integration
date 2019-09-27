package com.datasphere.TypeHandlers;

class ByteArrayToBlobHandler extends ByteArrayHandler
{
    public ByteArrayToBlobHandler(final String targetType) {
        super(OracleTypeHandler.BLOB, targetType);
    }
}
