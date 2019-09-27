package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class DateTimeToTimeHandler extends TypeHandler
{
    public DateTimeToTimeHandler(final String targetType) {
        super(92, DateTime.class, targetType);
    }
    
    public DateTimeToTimeHandler(final int sqlType, final String targetType) {
        super(sqlType, LocalTime.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindDateTimeToTimeHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final DateTime dateTime = (DateTime)value;
        final Time time = new Time(dateTime.getMillisOfDay());
        return "'" + time.toString() + "'";
    }
}
