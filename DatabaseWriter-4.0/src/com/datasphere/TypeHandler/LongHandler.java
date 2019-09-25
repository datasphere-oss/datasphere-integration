package com.datasphere.TypeHandler;

import java.sql.*;

class LongHandler extends TypeHandler
{
    public LongHandler() {
        super(4, Long.class);
    }
    
    public LongHandler(final int sqlType) {
        super(sqlType, Long.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setLong(bindIndex, (long)value);
    }
}
