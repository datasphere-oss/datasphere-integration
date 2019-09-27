package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToTinyIntHandler extends StringHandler
{
    public StringToTinyIntHandler(final String targetType) {
        super(-6, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindStringToTinyIntHandler(stmt, bindIndex, value);
    }
}
