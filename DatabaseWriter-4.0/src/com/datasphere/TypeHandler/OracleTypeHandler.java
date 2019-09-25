package com.datasphere.TypeHandler;

public class OracleTypeHandler extends TargetTypeHandler
{
    public static int BINARY_FLOAT;
    public static int BINARY_DOUBLE;
    public static int TIMESTAMP_WITH_TIMEZONE;
    public static int TIMESTAMP_WITH_LOCALTIMEZONE;
    public static int INTERVAL_YEAR_TO_MONTH;
    public static int INTERVAL_DAY_TO_SECOND;
    public static int BLOB;
    public static int CLOB;
    
    @Override
    public int getSqlType(final int sqlType, final int preceision) {
        if (sqlType != 1111) {
            return super.getSqlType(sqlType, preceision);
        }
        if (preceision == 0) {
            return 12;
        }
        return 93;
    }
    
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new StringToNCharHander());
        this.addToMap(new StringToVarCharHandler());
        this.addToMap(new StringToLongVarCharHandler());
        this.addToMap(new FloatToBinaryFloatHandler());
        this.addToMap(new FloatToBinaryDoubleHandler());
        this.addToMap(new DoubleToBinaryFloatHandler());
        this.addToMap(new DoubleToBinaryDoubleHandler());
        this.addToMap(new DateTimeToTiestampWithTimeZoneHandler());
        this.addToMap(new DateTimeToTimestampWithLocalTimezoneHandler());
        this.addToMap(new LocalDateToTimestampWithTimezoneHandler());
        this.addToMap(new LocalDateToTimestampWithLocalTimezoneHandler());
        this.addToMap(new ByteArrayToBlobHandler());
        this.addToMap(new HexStringToBlobHandler());
        this.addToMap(new StringToIntervalYearToMonthHandler());
        this.addToMap(new IntegerToIntervalYearToMonthHandler());
        this.addToMap(new StringToIntervalDayToSecondHandler());
        this.addToMap(new IntegerToIntervalDayToSecondHandler());
    }
    
    static {
        OracleTypeHandler.BINARY_FLOAT = 100;
        OracleTypeHandler.BINARY_DOUBLE = 101;
        OracleTypeHandler.TIMESTAMP_WITH_TIMEZONE = -101;
        OracleTypeHandler.TIMESTAMP_WITH_LOCALTIMEZONE = -102;
        OracleTypeHandler.INTERVAL_YEAR_TO_MONTH = -103;
        OracleTypeHandler.INTERVAL_DAY_TO_SECOND = -104;
        OracleTypeHandler.BLOB = 2004;
        OracleTypeHandler.CLOB = 2005;
    }
}
