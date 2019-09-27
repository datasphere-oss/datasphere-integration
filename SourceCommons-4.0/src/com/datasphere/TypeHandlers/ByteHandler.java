package com.datasphere.TypeHandlers;

import java.sql.*;

class ByteHandler extends TypeHandler
{
    public ByteHandler(final String targetType) {
        super(-2, Byte.class, targetType);
    }
    
    public ByteHandler(final int sqlType, final String targetType) {
        super(sqlType, Byte.class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindByteHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final byte byteData = (byte)value;
        return "'" + (char)byteData + "'";
    }
}
