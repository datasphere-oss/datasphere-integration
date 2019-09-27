package com.datasphere.TypeHandlers;

import java.sql.*;

class BooleanToIntegerHandler extends TypeHandler
{
    public BooleanToIntegerHandler(final String targetType) {
        super(4, Boolean.class, targetType);
    }
    
    public BooleanToIntegerHandler(final int sqlType, final String targetType) {
        super(sqlType, Boolean.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindBooleanToIntegerHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return ((Boolean)value).toString();
    }
}
