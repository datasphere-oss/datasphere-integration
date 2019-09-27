package com.datasphere.TypeHandlers;

import java.sql.*;

public class MSSQLTypeHandler extends TargetTypeHandler
{
    private final Integer SQL_VARIANT;
    public String targetType;
    
    public MSSQLTypeHandler(final String targetType) {
        super(targetType);
        this.SQL_VARIANT = -150;
        this.targetType = targetType;
        TypeHandler.unsupportedSQLTypes.put(this.SQL_VARIANT, this.SQL_VARIANT);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new ByteArrayToImageHandler(this.targetType));
        this.addToMap(new StringToTimestampHandler(this.targetType));
    }
    
    class ByteArrayToImageHandler extends ByteArrayHandler
    {
        public ByteArrayToImageHandler(final String targetType) {
            super(-4, targetType);
        }
    }
    
    class StringToTimestampHandler extends TypeHandler
    {
        public StringToTimestampHandler(final String targetType) {
            super(93, String.class, targetType);
        }
        
        public StringToTimestampHandler(final int sqlType) {
            super(sqlType, String.class, MSSQLTypeHandler.this.targetType);
        }
        
        public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
            final Timestamp timeStamp = Timestamp.valueOf((String)value);
            stmt.setTimestamp(bindIndex, timeStamp);
        }
    }
}
