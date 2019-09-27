package com.datasphere.TypeHandlers;

class ByteArrayToLongVarBinaryHandler extends ByteArrayHandler
{
    public ByteArrayToLongVarBinaryHandler(final String targetType) {
        super(-4, targetType);
    }
}
