package com.datasphere.TypeHandlers;

class HexStringToBlobHandler extends HexStringToBinaryHandler
{
    public HexStringToBlobHandler(final String targetType) {
        super(OracleTypeHandler.BLOB, targetType);
    }
}
