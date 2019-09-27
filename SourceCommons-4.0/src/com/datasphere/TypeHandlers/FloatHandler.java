package com.datasphere.TypeHandlers;

import java.sql.*;

class FloatHandler extends TypeHandler
{
    public FloatHandler(final String targetType) {
        super(6, Float.class, targetType);
    }
    
    public FloatHandler(final int sqlType, final String targetType) {
        super(sqlType, Float.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        try {
            this.bindIntf.bindFloatHandler(stmt, bindIndex, value);
        }
        catch (ClassCastException exp) {
            throw exp;
        }
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value;
    }
}
