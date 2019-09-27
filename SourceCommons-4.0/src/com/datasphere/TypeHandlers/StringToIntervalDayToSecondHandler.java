package com.datasphere.TypeHandlers;

class StringToIntervalDayToSecondHandler extends StringHandler
{
    public StringToIntervalDayToSecondHandler(final String targetType) {
        super(OracleTypeHandler.INTERVAL_DAY_TO_SECOND, targetType);
    }
}
