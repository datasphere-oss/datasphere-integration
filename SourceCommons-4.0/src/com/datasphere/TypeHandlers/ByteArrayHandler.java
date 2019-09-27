package com.datasphere.TypeHandlers;

import java.sql.*;
import java.io.*;

class ByteArrayHandler extends TypeHandler
{
    public ByteArrayHandler(final String targetType) {
        super(-2, byte[].class, targetType);
    }
    
    public ByteArrayHandler(final int sqlType, final String targetType) {
        super(sqlType, byte[].class, targetType);
    }
    
    @Override
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException {
        this.bindIntf.bindByteArrayHandler(stmt, bindIndex, value);
    }
    
    @Override
    public String toString(final Object value) {
        final byte[] byteData = (byte[])value;
        try {
            return "'" + new String(byteData, "UTF-8") + "'";
        }
        catch (UnsupportedEncodingException e) {
            return "Couldn't convert byte[] to Stiring";
        }
    }
}
