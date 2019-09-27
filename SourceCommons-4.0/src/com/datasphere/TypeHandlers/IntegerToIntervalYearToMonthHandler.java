package com.datasphere.TypeHandlers;

class IntegerToIntervalYearToMonthHandler extends IntegerHandler
{
    public IntegerToIntervalYearToMonthHandler(final String targetType) {
        super(OracleTypeHandler.INTERVAL_YEAR_TO_MONTH, targetType);
    }
}
