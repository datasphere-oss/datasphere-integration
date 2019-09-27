package com.datasphere.TypeHandlers;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Map;

import org.apache.kudu.client.PartialRow;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.datasphere.runtime.utils.StringUtils;

public class KuduTypeHandler implements TypeHandlerIntf
{
    @Override
    public void bindIntegerHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addInt(bindIndex, (int)value);
    }
    
    @Override
    public void bindShortHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addShort(bindIndex, (short)value);
    }
    
    @Override
    public void bindDefaultBooleanHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addBoolean(bindIndex, (boolean)value);
    }
    
    @Override
    public void bindStringHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        try {
            stmt.addString(bindIndex, (String)value);
        }
        catch (Exception exp) {
            throw exp;
        }
    }
    
    @Override
    public void bindStringToBinaryHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addBinary(bindIndex, ((String)value).getBytes());
    }
    
    @Override
    public void bindByteArrayHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addBinary(bindIndex, (byte[])value);
    }
    
    @Override
    public void bindByteHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addByte(bindIndex, (byte)value);
    }
    
    @Override
    public void bindDoubleHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addDouble(bindIndex, (double)value);
    }
    
    @Override
    public void bindFloatHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        try {
            stmt.addFloat(bindIndex, (float)value);
        }
        catch (ClassCastException exp) {
            throw exp;
        }
    }
    
    @Override
    public void bindLongHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addLong(bindIndex, (long)value);
    }
    
    @Override
    public void bindDateHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addLong(bindIndex, ((Date)value).getTime());
    }
    
    @Override
    public void bindDateTimeToTimestampHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Long timeStamp = ((DateTime)value).getMillis() * 1000L;
        stmt.addLong(bindIndex, (long)timeStamp);
    }
    
    @Override
    public void bindDateTimeToStringHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final String timeStamp = value.toString();
        stmt.addString(bindIndex, timeStamp);
    }
    
    @Override
    public void bindBooleanToIntegerHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final int colValue = ((boolean)value) ? 1 : 0;
        stmt.addInt(bindIndex, colValue);
    }
    
    @Override
    public void bindHexStringToBinaryHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addBinary(bindIndex, StringUtils.hexToRaw((String)value));
    }
    
    @Override
    public void bindDateTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final DateTime datetime = (DateTime)value;
        final Long date = datetime.getMillis() * 1000L;
        stmt.addLong(bindIndex, (long)date);
    }
    
    @Override
    public void bindTimestampHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addLong(bindIndex, (long)value);
    }
    
    @Override
    public void bindLocalDateHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final Long date = ((LocalDate)value).toDate().getTime();
        stmt.addLong(bindIndex, (long)date);
    }
    
    @Override
    public void bindLocalDateToTimestampHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final Long ts = ((LocalDate)value).toDateTimeAtStartOfDay().getMillis() * 1000L;
        stmt.addLong(bindIndex, (long)ts);
    }
    
    @Override
    public void bindLocalTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final LocalTime lt = (LocalTime)value;
        final Long time = lt.getMillisOfDay() * 1000L;
        stmt.addLong(bindIndex, (long)time);
    }
    
    @Override
    public void bindDateTimeToTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        final DateTime dateTime = (DateTime)value;
        final Long time = dateTime.getMillisOfDay() * 1000L;
        stmt.addLong(bindIndex, (long)time);
    }
    
    @Override
    public void bindStringToIntegerHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Integer intValue = Integer.parseInt(value.toString());
        stmt.addInt(bindIndex, (int)intValue);
    }
    
    @Override
    public void bindStringToTinyIntHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final String svalue = value.toString();
        final byte[] byteValue = svalue.getBytes();
        stmt.addByte(bindIndex, byteValue[0]);
    }
    
    @Override
    public void bindStringToSmallIntHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Short shortValue = Short.parseShort(value.toString());
        stmt.addShort(bindIndex, (short)shortValue);
    }
    
    @Override
    public void bindStringToBigIntHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Long shortValue = Long.parseLong(value.toString());
        stmt.addLong(bindIndex, (long)shortValue);
    }
    
    @Override
    public void bindStringToFloatHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Float shortValue = Float.parseFloat(value.toString());
        stmt.addFloat(bindIndex, (float)shortValue);
    }
    
    @Override
    public void bindStringToDoubleHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final Double shortValue = Double.parseDouble(value.toString());
        stmt.addDouble(bindIndex, (double)shortValue);
    }
    
    @Override
    public void bindNull(final Object bind, final int bindIndex, final Map<Integer, Integer> unsupportedSQLTypes, final int sqlType) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.setNull(bindIndex);
    }
    
    @Override
    public void bindStringToBooleanHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        final String boolString = value.toString();
        Boolean boolData;
        if (boolString.matches("(?i)0|f|fail|false")) {
            boolData = false;
        }
        else {
            boolData = true;
        }
        stmt.addBoolean(bindIndex, (boolean)boolData);
    }
    
    @Override
    public void bindBigIntToVarCharHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addString(bindIndex, value.toString());
    }
    
    @Override
    public void bindDoubleToFloatHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addFloat(bindIndex, Float.parseFloat(value.toString()));
    }
    
    @Override
    public void IntergerToLongHandler(final Object bind, final int bindIndex, final Object value) {
        final PartialRow stmt = (PartialRow)bind;
        stmt.addLong(bindIndex, Long.parseLong(value.toString()));
    }
}
