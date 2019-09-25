package com.datasphere.TypeHandler;

import java.sql.*;

class StringHandler extends TypeHandler
{
    public StringHandler() {
        super(1, String.class);
    }
    
    public StringHandler(final int sqlType) {
        super(sqlType, String.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        try {
            stmt.setString(bindIndex, (String)value);
        }
        catch (SQLException exp) {
            throw exp;
        }
    }
}
