package com.datasphere.TypeHandler;

import java.sql.*;

class ShortHandler extends TypeHandler
{
    public ShortHandler() {
        super(5, Short.class);
    }
    
    public ShortHandler(final int sqlType) {
        super(sqlType, Short.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setShort(bindIndex, (short)value);
    }
}
