package com.datasphere.TypeHandlers;

import java.sql.*;

class StringToFloatHandler extends StringHandler
{
    public StringToFloatHandler(final String targetType) {
        super(6, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        try {
            this.bindIntf.bindStringToFloatHandler(stmt, bindIndex, value);
        }
        catch (Exception exp) {
            throw exp;
        }
    }
}
