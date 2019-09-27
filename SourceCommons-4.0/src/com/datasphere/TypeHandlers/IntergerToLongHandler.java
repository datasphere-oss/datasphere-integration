package com.datasphere.TypeHandlers;

import java.sql.*;

class IntergerToLongHandler extends TypeHandler
{
    public IntergerToLongHandler(final String targetType) {
        super(-5, Integer.class, targetType);
    }
    
    public IntergerToLongHandler(final int sqlType, final String targetType) {
        super(sqlType, Integer.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.IntergerToLongHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value.toString();
    }
}
