package com.datasphere.TypeHandlers;

class DateTimeToTimestampWithLocalTimezoneHandler extends DateTimeToTimestampHandler
{
    public DateTimeToTimestampWithLocalTimezoneHandler(final String targetType) {
        super(OracleTypeHandler.TIMESTAMP_WITH_LOCALTIMEZONE, targetType);
    }
}
