package com.datasphere.TypeHandler;

class DateTimeToTiestampWithTimeZoneHandler extends DateTimeToTimestampHandler
{
    public DateTimeToTiestampWithTimeZoneHandler() {
        super(OracleTypeHandler.TIMESTAMP_WITH_TIMEZONE);
    }
}
