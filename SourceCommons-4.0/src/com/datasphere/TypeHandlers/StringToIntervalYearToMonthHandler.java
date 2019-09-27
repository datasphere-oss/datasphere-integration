package com.datasphere.TypeHandlers;

class StringToIntervalYearToMonthHandler extends StringHandler
{
    public StringToIntervalYearToMonthHandler(final String targetType) {
        super(OracleTypeHandler.INTERVAL_YEAR_TO_MONTH, targetType);
    }
}
