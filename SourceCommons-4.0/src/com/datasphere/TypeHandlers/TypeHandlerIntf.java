package com.datasphere.TypeHandlers;

import java.sql.*;
import java.util.*;

public interface TypeHandlerIntf
{
    void bindIntegerHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindShortHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDefaultBooleanHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindStringHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindStringToBinaryHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindByteArrayHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindByteHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDoubleHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindFloatHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindLongHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDateHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDateTimeToTimestampHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindBooleanToIntegerHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindHexStringToBinaryHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDateTimeHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindTimestampHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindLocalDateHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindLocalDateToTimestampHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindLocalTimeHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDateTimeToTimeHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindStringToIntegerHandler(final Object p0, final int p1, final Object p2);
    
    void bindStringToTinyIntHandler(final Object p0, final int p1, final Object p2);
    
    void bindStringToSmallIntHandler(final Object p0, final int p1, final Object p2);
    
    void bindStringToBigIntHandler(final Object p0, final int p1, final Object p2);
    
    void bindStringToFloatHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindStringToDoubleHandler(final Object p0, final int p1, final Object p2);
    
    void bindNull(final Object p0, final int p1, final Map<Integer, Integer> p2, final int p3) throws SQLException;
    
    void bindStringToBooleanHandler(final Object p0, final int p1, final Object p2);
    
    void bindDateTimeToStringHandler(final Object p0, final int p1, final Object p2);
    
    void bindBigIntToVarCharHandler(final Object p0, final int p1, final Object p2) throws SQLException;
    
    void bindDoubleToFloatHandler(final Object p0, final int p1, final Object p2);
    
    void IntergerToLongHandler(final Object p0, final int p1, final Object p2);
}
