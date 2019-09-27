package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class DateTimeHandler extends DateHandler
{
    public DateTimeHandler(final String targetType) {
        super(91, DateTime.class, targetType);
    }
    
    public DateTimeHandler(final int sqlType, final String targetType) {
        super(sqlType, Date.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDateTimeHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final DateTime datetime = (DateTime)value;
        final Date date = new Date(datetime.getMillis());
        return "'" + date.toString() + "'";
    }
}
