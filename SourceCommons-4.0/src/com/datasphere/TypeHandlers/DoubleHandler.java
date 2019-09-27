package com.datasphere.TypeHandlers;

import java.sql.*;

class DoubleHandler extends TypeHandler
{
    public DoubleHandler(final String targetType) {
        super(8, Double.class, targetType);
    }
    
    public DoubleHandler(final int sqlType, final String targetType) {
        super(sqlType, Double.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDoubleHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value;
    }
}
