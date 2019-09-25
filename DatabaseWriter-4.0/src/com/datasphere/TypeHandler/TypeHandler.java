package com.datasphere.TypeHandler;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.exception.UpdateTypeMapException;

public class TypeHandler
{
    private static Logger logger;
    public static String SEPARATOR;
    int sqlType;
    public static Map<Integer, Integer> unsupportedSQLTypes;
    Class<?> srcObjType;
    
    public TypeHandler() {
    }
    
    public TypeHandler(final int sqlType) {
        this.sqlType = sqlType;
    }
    
    public TypeHandler(final int sqlType, final Class<?> sourceType) {
        this.sqlType = sqlType;
        this.srcObjType = sourceType;
    }
    
    public void bind(final PreparedStatement stmt, final int bindIndex, final Object value) throws SQLException, UpdateTypeMapException {
        throw new UpdateTypeMapException(bindIndex, value.getClass());
    }
    
    public void bindNull(final PreparedStatement stmt, final int bindIndex) throws SQLException {
        stmt.setNull(bindIndex, this.sqlType);
    }
    
    public int getSqlType(final int sqlType, final int precision) {
        return sqlType;
    }
    
    public String toString(final Object value) {
        return "null";
    }
    
    @Override
    public String toString() {
        return this.sqlType + TypeHandler.SEPARATOR + this.srcObjType.getName();
    }
    
    static {
        TypeHandler.logger = LoggerFactory.getLogger((Class)TypeHandler.class);
        TypeHandler.SEPARATOR = "-";
        TypeHandler.unsupportedSQLTypes = new HashMap<Integer, Integer>();
    }
}
