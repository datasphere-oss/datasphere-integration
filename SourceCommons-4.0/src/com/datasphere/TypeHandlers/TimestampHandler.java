package com.datasphere.TypeHandlers;

import java.sql.*;

class TimestampHandler extends TypeHandler
{
    public TimestampHandler(final String targetType) {
        super(93, Timestamp.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindTimestampHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "'" + ((Timestamp)value).toString() + "'";
    }
}
