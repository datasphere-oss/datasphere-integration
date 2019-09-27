package com.datasphere.TypeHandlers;

class FloatToBinaryDoubleHandler extends FloatHandler
{
    public FloatToBinaryDoubleHandler(final String targetType) {
        super(OracleTypeHandler.BINARY_DOUBLE, targetType);
    }
}
