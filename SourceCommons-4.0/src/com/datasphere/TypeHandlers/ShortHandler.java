package com.datasphere.TypeHandlers;

import java.sql.*;

class ShortHandler extends TypeHandler
{
    public ShortHandler(final String targetType) {
        super(5, Short.class, targetType);
    }
    
    public ShortHandler(final int sqlType, final String targetType) {
        super(sqlType, Short.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindShortHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value;
    }
}
