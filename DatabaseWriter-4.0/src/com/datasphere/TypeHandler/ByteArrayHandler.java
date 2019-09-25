package com.datasphere.TypeHandler;

import java.sql.*;

class ByteArrayHandler extends TypeHandler
{
    public ByteArrayHandler() {
        super(-2, byte[].class);
    }
    
    public ByteArrayHandler(final int sqlType) {
        super(sqlType, byte[].class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setBytes(bindIndex, (byte[])value);
    }
}
