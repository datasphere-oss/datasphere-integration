package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class LocalTimeHandler extends TypeHandler
{
    public LocalTimeHandler() {
        super(92, LocalTime.class);
    }
    
    public LocalTimeHandler(final int sqlType) {
        super(sqlType, LocalTime.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final LocalTime lt = (LocalTime)value;
        final Time time = new Time(lt.getMillisOfDay());
        stmt.setTime(bindIndex, time);
    }
}
