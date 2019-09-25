package com.datasphere.TypeHandler;

import java.sql.*;

class FloatHandler extends TypeHandler
{
    public FloatHandler() {
        super(6, Float.class);
    }
    
    public FloatHandler(final int sqlType) {
        super(sqlType, Float.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        try {
            stmt.setFloat(bindIndex, (float)value);
        }
        catch (ClassCastException exp) {
            throw exp;
        }
    }
}
