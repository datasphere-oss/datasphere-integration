package com.datasphere.TypeHandlers;

import java.sql.*;

class DoubleToFloatHandler extends TypeHandler
{
    public DoubleToFloatHandler(final String targetType) {
        super(6, Double.class, targetType);
    }
    
    public DoubleToFloatHandler(final int sqlType, final String targetType) {
        super(sqlType, Double.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDoubleToFloatHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value.toString();
    }
}
