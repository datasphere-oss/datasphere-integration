package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToIntegerHandler extends StringHandler
{
    public StringToIntegerHandler(final String targetType) {
        super(4, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToIntegerHandler(stmt, bindIndex, value);
    }
}
