package com.datasphere.TypeHandlers;

import com.datasphere.source.lib.meta.*;

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
    public String targetType;
    
    public OracleTypeHandler(final String targetType) {
        super(targetType);
        this.targetType = targetType;
    }
    
    @Override
    public int getSqlType(final int sqlType, final DatabaseColumn columnDetails) {
        final int preceision = columnDetails.getPrecision();
        if (sqlType != 1111) {
            return super.getSqlType(sqlType, columnDetails);
        }
        if (preceision == 0) {
            return 12;
        }
        return 93;
    }
    
    @Override
    public void initialize() {
        super.initialize();
        this.addToMap(new StringToNCharHander(this.targetType));
        this.addToMap(new StringToLongVarCharHandler(this.targetType));
        this.addToMap(new FloatToBinaryFloatHandler(this.targetType));
        this.addToMap(new FloatToBinaryDoubleHandler(this.targetType));
        this.addToMap(new DoubleToBinaryFloatHandler(this.targetType));
        this.addToMap(new DoubleToBinaryDoubleHandler(this.targetType));
        this.addToMap(new DateTimeToTiestampWithTimeZoneHandler(this.targetType));
        this.addToMap(new DateTimeToTimestampWithLocalTimezoneHandler(this.targetType));
        this.addToMap(new LocalDateToTimestampWithTimezoneHandler(this.targetType));
        this.addToMap(new LocalDateToTimestampWithLocalTimezoneHandler(this.targetType));
        this.addToMap(new ByteArrayToBlobHandler(this.targetType));
        this.addToMap(new HexStringToBlobHandler(this.targetType));
        this.addToMap(new StringToIntervalYearToMonthHandler(this.targetType));
        this.addToMap(new IntegerToIntervalYearToMonthHandler(this.targetType));
        this.addToMap(new StringToIntervalDayToSecondHandler(this.targetType));
        this.addToMap(new IntegerToIntervalDayToSecondHandler(this.targetType));
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
