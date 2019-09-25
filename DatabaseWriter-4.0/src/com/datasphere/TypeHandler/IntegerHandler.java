package com.datasphere.TypeHandler;

import java.sql.*;

class IntegerHandler extends TypeHandler
{
    public IntegerHandler() {
        super(4, Integer.class);
    }
    
    public IntegerHandler(final int sqlType) {
        super(sqlType, Integer.class);
    }
    
    public IntegerHandler(final int sqlType, final Class<?> Class) {
        super(sqlType, Class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setInt(bindIndex, (int)value);
    }
}
