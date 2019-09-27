package com.datasphere.TypeHandlers;

class ByteArrayToVarBinaryHandler extends ByteArrayHandler
{
    public ByteArrayToVarBinaryHandler(final String targetType) {
        super(-3, targetType);
    }
}
