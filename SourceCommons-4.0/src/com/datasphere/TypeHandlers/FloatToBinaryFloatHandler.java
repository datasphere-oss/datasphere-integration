package com.datasphere.TypeHandlers;

class FloatToBinaryFloatHandler extends FloatHandler
{
    public FloatToBinaryFloatHandler(final String targetType) {
        super(OracleTypeHandler.BINARY_FLOAT, targetType);
    }
}
