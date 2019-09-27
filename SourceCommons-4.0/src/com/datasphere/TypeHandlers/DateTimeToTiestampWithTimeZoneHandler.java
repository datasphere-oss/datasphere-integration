package com.datasphere.TypeHandlers;

class DateTimeToTiestampWithTimeZoneHandler extends DateTimeToTimestampHandler
{
    public DateTimeToTiestampWithTimeZoneHandler(final String targetType) {
        super(OracleTypeHandler.TIMESTAMP_WITH_TIMEZONE, targetType);
    }
}
