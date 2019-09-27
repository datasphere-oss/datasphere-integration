package com.datasphere.TypeHandlers;

class HexStringToLongVarBinary extends HexStringToBinaryHandler
{
    public HexStringToLongVarBinary(final String targetType) {
        super(-4, targetType);
    }
}
