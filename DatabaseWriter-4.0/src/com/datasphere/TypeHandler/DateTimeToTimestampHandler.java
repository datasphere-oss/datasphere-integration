package com.datasphere.TypeHandler;

import org.joda.time.*;
import java.sql.*;

class DateTimeToTimestampHandler extends TypeHandler
{
    public DateTimeToTimestampHandler() {
        super(93, DateTime.class);
    }
    
    public DateTimeToTimestampHandler(final int sqlType) {
        super(sqlType, DateTime.class);
    }
    
    @Override
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException {
        final Timestamp timeStamp = new Timestamp(((DateTime)value).getMillis());
        //System.out.println("----bindIndex:"+bindIndex + " timeStamp:"+timeStamp);
        stmt.setTimestamp(bindIndex, timeStamp);
    }
}
