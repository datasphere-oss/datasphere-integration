package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToBooHandler extends StringHandler
{
    public StringToBooHandler(final String targetType) {
        super(16, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToBooleanHandler(stmt, bindIndex, value);
    }
}
