package com.datasphere.TypeHandlers;

class HexStringToVarBinary extends HexStringToBinaryHandler
{
    public HexStringToVarBinary(final String targetType) {
        super(-3, targetType);
    }
}
