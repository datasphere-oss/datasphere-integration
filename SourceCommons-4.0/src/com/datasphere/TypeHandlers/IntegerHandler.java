package com.datasphere.TypeHandlers;

import java.sql.*;

class IntegerHandler extends TypeHandler
{
    public IntegerHandler(final String targetType) {
        super(4, Integer.class, targetType);
    }
    
    public IntegerHandler(final int sqlType, final String targetType) {
        super(sqlType, Integer.class, targetType);
    }
    
    public IntegerHandler(final int sqlType, final Class<?> Class, final String targetType) {
        super(sqlType, Class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindIntegerHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value;
    }
}
