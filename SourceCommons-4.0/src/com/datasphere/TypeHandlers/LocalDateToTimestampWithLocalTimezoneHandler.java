package com.datasphere.TypeHandlers;

class LocalDateToTimestampWithLocalTimezoneHandler extends LocalDateToTimestampHandler
{
    public LocalDateToTimestampWithLocalTimezoneHandler(final String targetType) {
        super(OracleTypeHandler.TIMESTAMP_WITH_LOCALTIMEZONE, targetType);
    }
}
