package com.datasphere.TypeHandlers;

import java.sql.*;

class StringHandler extends TypeHandler
{
    public StringHandler(final String targetType) {
        super(1, String.class, targetType);
    }
    
    public StringHandler(final int sqlType, final String targetType) {
        super(sqlType, String.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        try {
            this.bindIntf.bindStringHandler(stmt, bindIndex, value);
        }
        catch (Exception exp) {
            throw exp;
        }
    }
    
    @Override
    public String toString(final Object value) {
        return "'" + (String)value + "'";
    }
}
