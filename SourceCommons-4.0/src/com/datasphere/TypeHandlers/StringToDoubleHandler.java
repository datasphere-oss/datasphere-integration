package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToDoubleHandler extends StringHandler
{
    public StringToDoubleHandler(final String targetType) {
        super(8, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToDoubleHandler(stmt, bindIndex, value);
    }
}
