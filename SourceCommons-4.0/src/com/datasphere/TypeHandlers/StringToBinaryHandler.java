package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToBinaryHandler extends StringHandler
{
    public StringToBinaryHandler(final String targetType) {
        super(-2, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToBinaryHandler(stmt, bindIndex, value);
    }
}
