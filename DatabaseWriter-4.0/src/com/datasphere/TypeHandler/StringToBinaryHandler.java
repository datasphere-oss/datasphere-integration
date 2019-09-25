package com.datasphere.TypeHandler;

import java.sql.*;

class StringToBinaryHandler extends StringHandler
{
    public StringToBinaryHandler() {
        super(-2);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setBytes(bindIndex, ((String)value).getBytes());
    }
}
