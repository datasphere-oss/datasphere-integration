package com.datasphere.TypeHandlers;

class IntegerToIntervalDayToSecondHandler extends IntegerHandler
{
    public IntegerToIntervalDayToSecondHandler(final String targetType) {
        super(OracleTypeHandler.INTERVAL_DAY_TO_SECOND, targetType);
    }
}
