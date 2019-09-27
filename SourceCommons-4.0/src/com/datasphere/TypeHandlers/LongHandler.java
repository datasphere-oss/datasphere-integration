package com.datasphere.TypeHandlers;

import java.sql.*;

class LongHandler extends TypeHandler
{
    public LongHandler(final String targetType) {
        super(4, Long.class, targetType);
    }
    
    public LongHandler(final int sqlType, final String targetType) {
        super(sqlType, Long.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindLongHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "" + value;
    }
}
