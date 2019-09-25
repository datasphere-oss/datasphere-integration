package com.datasphere.TypeHandler;

class LocalDateToTimestampWithLocalTimezoneHandler extends LocalDateToTimestampHandler
{
    public LocalDateToTimestampWithLocalTimezoneHandler() {
        super(OracleTypeHandler.TIMESTAMP_WITH_LOCALTIMEZONE);
    }
}
