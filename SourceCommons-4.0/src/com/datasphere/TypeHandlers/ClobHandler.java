package com.datasphere.TypeHandlers;

class ClobHandler extends StringHandler
{
    public ClobHandler(final String targetType) {
        super(OracleTypeHandler.CLOB, targetType);
    }
    
    @Override
    public String toString(final Object value) {
        return "<CLOB Value>";
    }
}
