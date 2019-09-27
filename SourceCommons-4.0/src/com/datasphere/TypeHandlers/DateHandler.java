package com.datasphere.TypeHandlers;

import java.sql.*;

class DateHandler extends TypeHandler
{
    public DateHandler(final String targetType) {
        super(91, Date.class, targetType);
    }
    
    public DateHandler(final int sqlType, final Class<?> Class, final String targetType) {
        super(sqlType, Class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDateHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return ((Date)value).toString();
    }
}
