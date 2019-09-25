package com.datasphere.TypeHandler;

import java.sql.*;

class DoubleHandler extends TypeHandler
{
    public DoubleHandler() {
        super(8, Double.class);
    }
    
    public DoubleHandler(final int sqlType) {
        super(sqlType, Double.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setDouble(bindIndex, (double)value);
    }
}
