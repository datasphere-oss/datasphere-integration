package com.datasphere.TypeHandlers;

import org.apache.log4j.*;

import com.datasphere.Exceptions.*;

import java.sql.*;
import java.util.*;

public class TypeHandler implements Cloneable
{
    private static Logger logger;
    public static String SEPARATOR;
    public TypeHandlerIntf bindIntf;
    int sqlType;
    public static Map<Integer, Integer> unsupportedSQLTypes;
    Class<?> srcObjType;
    
    public TypeHandler(final String targetType) {
        if (targetType.equals("kudu")) {
            this.bindIntf = new KuduTypeHandler();
        }
        else {
            this.bindIntf = new JdbcTypeHandler();
        }
    }
    
    public TypeHandler(final int sqlType, final String targetType) {
        this.sqlType = sqlType;
        if (targetType.equals("kudu")) {
            this.bindIntf = new KuduTypeHandler();
        }
        else {
            this.bindIntf = new JdbcTypeHandler();
        }
    }
    
    public TypeHandler(final int sqlType, final Class<?> sourceType, final String targetType) {
        this.sqlType = sqlType;
        this.srcObjType = sourceType;
        if (targetType.equals("kudu")) {
            this.bindIntf = new KuduTypeHandler();
        }
        else {
            this.bindIntf = new JdbcTypeHandler();
        }
    }
    
    public void bind(final Object stmt, final int bindIndex, final Object value) throws SQLException, UpdateTypeMapException {
        throw new UpdateTypeMapException(bindIndex, value.getClass());
    }
    
    public void bindNull(final Object stmt, final int bindIndex) throws SQLException {
        this.bindIntf.bindNull(stmt, bindIndex, TypeHandler.unsupportedSQLTypes, this.sqlType);
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
    
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
    
    static {
        TypeHandler.logger = Logger.getLogger((Class)TypeHandler.class);
        TypeHandler.SEPARATOR = "-";
        TypeHandler.unsupportedSQLTypes = new HashMap<Integer, Integer>();
    }
}
