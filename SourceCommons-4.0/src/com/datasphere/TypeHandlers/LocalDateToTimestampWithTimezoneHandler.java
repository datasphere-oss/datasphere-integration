package com.datasphere.TypeHandlers;

class LocalDateToTimestampWithTimezoneHandler extends LocalDateToTimestampHandler
{
    public LocalDateToTimestampWithTimezoneHandler(final String targetType) {
        super(OracleTypeHandler.TIMESTAMP_WITH_TIMEZONE, targetType);
    }
}
