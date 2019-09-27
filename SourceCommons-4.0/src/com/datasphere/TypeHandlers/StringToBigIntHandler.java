package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToBigIntHandler extends StringHandler
{
    public StringToBigIntHandler(final String targetType) {
        super(-5, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToBigIntHandler(stmt, bindIndex, value);
    }
}
