package com.datasphere.TypeHandlers;

class DoubleToBinaryDoubleHandler extends DoubleHandler
{
    public DoubleToBinaryDoubleHandler(final String targetType) {
        super(OracleTypeHandler.BINARY_DOUBLE, targetType);
    }
}
