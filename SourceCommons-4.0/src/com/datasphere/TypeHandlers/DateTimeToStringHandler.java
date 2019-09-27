package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class DateTimeToStringHandler extends TypeHandler
{
    public DateTimeToStringHandler(final String targetType) {
        super(12, DateTime.class, targetType);
    }
    
    public DateTimeToStringHandler(final int sqlType, final String targetType) {
        super(sqlType, DateTime.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDateTimeToStringHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final Timestamp timeStamp = new Timestamp(((DateTime)value).getMillis());
        return "'" + timeStamp.toString() + "'";
    }
}
