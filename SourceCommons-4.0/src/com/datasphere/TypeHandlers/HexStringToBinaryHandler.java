package com.datasphere.TypeHandlers;

import java.sql.*;

class HexStringToBinaryHandler extends TypeHandler
{
    public HexStringToBinaryHandler(final String targetType) {
        super(-2, String.class, targetType);
    }
    
    public HexStringToBinaryHandler(final int sqlType, final String targetType) {
        super(sqlType, String.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindHexStringToBinaryHandler(stmt, bindIndex, value);
    }
}
