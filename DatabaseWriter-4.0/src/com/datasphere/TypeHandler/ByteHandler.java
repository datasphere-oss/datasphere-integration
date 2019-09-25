package com.datasphere.TypeHandler;

import java.sql.*;

class ByteHandler extends TypeHandler
{
    public ByteHandler() {
        super(-2, Byte.class);
    }
    
    public ByteHandler(final int sqlType) {
        super(sqlType, Byte.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        stmt.setByte(bindIndex, (byte)value);
    }
}
