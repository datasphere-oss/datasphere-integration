package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class LocalDateHandler extends TypeHandler
{
    public LocalDateHandler() {
        super(91, LocalDate.class);
    }
    
    public LocalDateHandler(final int sqlType) {
        super(sqlType, LocalDate.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final Date date = new Date(((LocalDate)value).toDate().getTime());
        stmt.setDate(bindIndex, date);
    }
}
