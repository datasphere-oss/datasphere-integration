package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class LocalDateToTimestampHandler extends TypeHandler
{
    public LocalDateToTimestampHandler(final String targetType) {
        super(93, LocalDate.class, targetType);
    }
    
    public LocalDateToTimestampHandler(final int sqlType, final String targetType) {
        super(sqlType, LocalDate.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindLocalDateToTimestampHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final Timestamp ts = new Timestamp(((LocalDate)value).toDateTimeAtStartOfDay().getMillis());
        return "'" + ts.toString() + "'";
    }
}
