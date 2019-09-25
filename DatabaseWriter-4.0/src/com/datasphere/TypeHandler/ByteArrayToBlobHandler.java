package com.datasphere.TypeHandler;

class ByteArrayToBlobHandler extends ByteArrayHandler
{
    public ByteArrayToBlobHandler() {
        super(OracleTypeHandler.BLOB);
    }
}
