package com.datasphere.TypeHandler;

class DateTimeToTimestampWithLocalTimezoneHandler extends DateTimeToTimestampHandler
{
    public DateTimeToTimestampWithLocalTimezoneHandler() {
        super(OracleTypeHandler.TIMESTAMP_WITH_LOCALTIMEZONE);
    }
}
