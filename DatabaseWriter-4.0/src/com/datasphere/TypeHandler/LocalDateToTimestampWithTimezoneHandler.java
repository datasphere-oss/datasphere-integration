package com.datasphere.TypeHandler;

class LocalDateToTimestampWithTimezoneHandler extends LocalDateToTimestampHandler
{
    public LocalDateToTimestampWithTimezoneHandler() {
        super(OracleTypeHandler.TIMESTAMP_WITH_TIMEZONE);
    }
}
