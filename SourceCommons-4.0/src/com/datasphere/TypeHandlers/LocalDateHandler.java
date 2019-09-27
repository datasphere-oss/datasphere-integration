package com.datasphere.TypeHandlers;

import org.joda.time.*;
import java.sql.*;

class LocalDateHandler extends TypeHandler
{
    public LocalDateHandler(final String targetType) {
        super(91, LocalDate.class, targetType);
    }
    
    public LocalDateHandler(final int sqlType, final String targetType) {
        super(sqlType, LocalDate.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindLocalDateHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        return "'" + ((LocalDate)value).toString() + "'";
    }
}
