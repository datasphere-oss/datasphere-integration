package com.datasphere.TypeHandler;

import java.sql.*;

class DateHandler extends TypeHandler
{
    public DateHandler() {
        super(91, Date.class);
    }
    
    public DateHandler(final int sqlType, final Class<?> Class) {
        super(sqlType, Class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setDate(bindIndex, (Date)value);
    }
}
