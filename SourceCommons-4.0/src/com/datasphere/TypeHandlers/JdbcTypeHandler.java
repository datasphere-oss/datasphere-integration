package com.datasphere.TypeHandlers;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.datasphere.runtime.utils.StringUtils;

public class JdbcTypeHandler implements TypeHandlerIntf
{
    @Override
    public void bindIntegerHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setInt(bindIndex, (int)value);
    }
    
    @Override
    public void bindShortHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setShort(bindIndex, (short)value);
    }
    
    @Override
    public void bindDefaultBooleanHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setBoolean(bindIndex, (boolean)value);
    }
    
    @Override
    public void bindStringHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        try {
            stmt.setString(bindIndex, (String)value);
        }
        catch (SQLException exp) {
            throw exp;
        }
    }
    
    @Override
    public void bindStringToBinaryHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setBytes(bindIndex, ((String)value).getBytes());
    }
    
    @Override
    public void bindByteArrayHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setBytes(bindIndex, (byte[])value);
    }
    
    @Override
    public void bindByteHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setByte(bindIndex, (byte)value);
    }
    
    @Override
    public void bindDoubleHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setDouble(bindIndex, (double)value);
    }
    
    @Override
    public void bindFloatHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        try {
            stmt.setFloat(bindIndex, (float)value);
        }
        catch (ClassCastException exp) {
            throw exp;
        }
    }
    
    @Override
    public void bindLongHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setLong(bindIndex, (long)value);
    }
    
    @Override
    public void bindDateHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setDate(bindIndex, (Date)value);
    }
    
    @Override
    public void bindDateTimeToTimestampHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final Timestamp timeStamp = new Timestamp(((DateTime)value).getMillis());
        stmt.setTimestamp(bindIndex, timeStamp);
    }
    
    @Override
    public void bindBooleanToIntegerHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final int colValue = ((boolean)value) ? 1 : 0;
        stmt.setInt(bindIndex, colValue);
    }
    
    @Override
    public void bindHexStringToBinaryHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setBytes(bindIndex, StringUtils.hexToRaw((String)value));
    }
    
    @Override
    public void bindDateTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final DateTime datetime = (DateTime)value;
        final Date date = new Date(datetime.getMillis());
        stmt.setDate(bindIndex, date);
    }
    
    @Override
    public void bindTimestampHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setTimestamp(bindIndex, (Timestamp)value);
    }
    
    @Override
    public void bindLocalDateHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final Date date = new Date(((LocalDate)value).toDate().getTime());
        stmt.setDate(bindIndex, date);
    }
    
    @Override
    public void bindLocalDateToTimestampHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final Timestamp ts = new Timestamp(((LocalDate)value).toDateTimeAtStartOfDay().getMillis());
        stmt.setTimestamp(bindIndex, ts);
    }
    
    @Override
    public void bindLocalTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final LocalTime lt = (LocalTime)value;
        final Time time = new Time(lt.getMillisOfDay());
        stmt.setTime(bindIndex, time);
    }
    
    @Override
    public void bindDateTimeToTimeHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        final DateTime dateTime = (DateTime)value;
        final Time time = new Time(dateTime.getMillisOfDay());
        stmt.setTime(bindIndex, time);
    }
    
    @Override
    public void bindStringToIntegerHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindStringToTinyIntHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindStringToSmallIntHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindStringToBigIntHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindStringToFloatHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        try {
            stmt.setString(bindIndex, (String)value);
        }
        catch (SQLException exp) {
            throw exp;
        }
    }
    
    @Override
    public void bindStringToDoubleHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindNull(final Object bind, final int bindIndex, final Map<Integer, Integer> unsupportedSQLTypes, final int sqlType) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        if (unsupportedSQLTypes.containsKey(sqlType)) {
            stmt.setNull(bindIndex, 0);
        }
        else {
            stmt.setNull(bindIndex, sqlType);
        }
    }
    
    @Override
    public void bindStringToBooleanHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindDateTimeToStringHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void bindBigIntToVarCharHandler(final Object bind, final int bindIndex, final Object value) throws SQLException {
        final PreparedStatement stmt = (PreparedStatement)bind;
        stmt.setString(bindIndex, value.toString());
    }
    
    @Override
    public void bindDoubleToFloatHandler(final Object stmt, final int bindIndex, final Object value) {
    }
    
    @Override
    public void IntergerToLongHandler(final Object stmt, final int bindIndex, final Object value) {
    }
}
