package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class DateTimeType12Handler extends DateHandler
{
    public DateTimeType12Handler() {
        super(12, DateTime.class);
    }
    
    public DateTimeType12Handler(final int sqlType) {
        super(sqlType, Date.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final DateTime datetime = (DateTime)value;
        final Date date = new Date(datetime.getMillis());
        super.bind(stmt, bindIndex, date);
    }
}
