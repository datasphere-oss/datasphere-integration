package com.datasphere.TypeHandlers;

class DoubleToBinaryFloatHandler extends DoubleHandler
{
    public DoubleToBinaryFloatHandler(final String targetType) {
        super(OracleTypeHandler.BINARY_FLOAT, targetType);
    }
}
