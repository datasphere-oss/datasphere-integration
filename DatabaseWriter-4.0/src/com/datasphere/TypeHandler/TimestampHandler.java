package com.datasphere.TypeHandler;

import java.sql.*;

class TimestampHandler extends TypeHandler
{
    public TimestampHandler() {
        super(93, Timestamp.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setTimestamp(bindIndex, (Timestamp)value);
    }
}
