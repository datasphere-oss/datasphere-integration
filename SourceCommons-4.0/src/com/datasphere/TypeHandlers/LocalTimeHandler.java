package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class LocalTimeHandler extends TypeHandler
{
    public LocalTimeHandler(final String targetType) {
        super(92, LocalTime.class, targetType);
    }
    
    public LocalTimeHandler(final int sqlType, final String targetType) {
        super(sqlType, LocalTime.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindLocalTimeHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final LocalTime lt = (LocalTime)value;
        final Time time = new Time(lt.getMillisOfDay());
        return "'" + time.toString() + "'";
    }
}
