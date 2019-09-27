package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToShortHandler extends StringHandler
{
    public StringToShortHandler(final String targetType) {
        super(5, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToSmallIntHandler(stmt, bindIndex, value);
    }
}
