package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class DateTimeToTimeHandler extends TypeHandler
{
    public DateTimeToTimeHandler() {
        super(92, DateTime.class);
    }
    
    public DateTimeToTimeHandler(final int sqlType) {
        super(sqlType, LocalTime.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final DateTime dateTime = (DateTime)value;
        final Time time = new Time(dateTime.getMillisOfDay());

        stmt.setTime(bindIndex, time);
    }
}
