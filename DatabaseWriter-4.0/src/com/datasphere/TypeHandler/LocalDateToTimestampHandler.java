package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class LocalDateToTimestampHandler extends TypeHandler
{
    public LocalDateToTimestampHandler() {
        super(93, LocalDate.class);
    }
    
    public LocalDateToTimestampHandler(final int sqlType) {
        super(sqlType, LocalDate.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final Timestamp ts = new Timestamp(((LocalDate)value).toDateTimeAtStartOfDay().getMillis());
        stmt.setTimestamp(bindIndex, ts);
    }
}
