package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class DateTimeHandler extends DateHandler
{
    public DateTimeHandler() {
        super(91, DateTime.class);
    }
    
    public DateTimeHandler(final int sqlType) {
        super(sqlType, Date.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final DateTime datetime = (DateTime)value;
        final Date date = new Date(datetime.getMillis());
        super.bind(stmt, bindIndex, date);
    }
}
