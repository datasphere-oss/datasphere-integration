package com.datasphere.TypeHandler;

import java.sql.*;

public class MSSQLTypeHandler extends TargetTypeHandler
{
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new ByteArrayToImageHandler());
        this.addToMap(new StringToTimestampHandler());
    }
    
    class ByteArrayToImageHandler extends ByteArrayHandler
    {
        public ByteArrayToImageHandler() {
            super(-4);
        }
    }
    
    class StringToTimestampHandler extends TypeHandler
    {
        public StringToTimestampHandler() {
            super(93, String.class);
        }
        
        public StringToTimestampHandler(final int sqlType) {
            super(sqlType, String.class);
        }
        
        @Override
        public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
            final Timestamp timeStamp = Timestamp.valueOf((String)value);
            stmt.setTimestamp(bindIndex, timeStamp);
        }
    }
}
