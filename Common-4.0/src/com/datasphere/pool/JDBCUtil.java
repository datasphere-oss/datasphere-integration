package com.datasphere.pool;

import java.util.*;

public class JDBCUtil
{
    public static Map<String, String> driverMap;
    public static Map<String, DB> utilClass;
    
    public static String getDriverClassName(final DBResourcePool.DB_TYPE dbtype) {
        final String name = dbtype.name();
        if (name == null || name.isEmpty()) {
            return name;
        }
        if (JDBCUtil.driverMap.containsKey(name)) {
            return JDBCUtil.driverMap.get(name);
        }
        return "<Unknown database given!!!>";
    }
    
    public static DB getHelpClass(final String name) {
        if (name == null || name.isEmpty()) {
            return new DB();
        }
        if (JDBCUtil.driverMap.containsKey(name) && JDBCUtil.utilClass.containsKey(name)) {
            return JDBCUtil.utilClass.get(name);
        }
        return new DB();
    }
    
    static {
        JDBCUtil.driverMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        JDBCUtil.utilClass = new TreeMap<String, DB>(String.CASE_INSENSITIVE_ORDER);
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.MYSQL.name(), "com.mysql.jdbc.Driver");
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.ORACLE.name(), "oracle.jdbc.driver.OracleDriver");
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.MSSQL.name(), "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.SYBASE.name(), "ncom.sybase.jdbc2.jdbc.SybDriver");
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.POSTGRESQL.name(), "org.postgresql.Driver");
        JDBCUtil.driverMap.put(DBResourcePool.DB_TYPE.DERBY.name(), "org.apache.derby.jdbc.ClientDriver");
        JDBCUtil.utilClass.put(DBResourcePool.DB_TYPE.MYSQL.name(), new MYSQL());
        JDBCUtil.utilClass.put(DBResourcePool.DB_TYPE.MSSQL.name(), new MSSQL());
    }
    
    public static class DB
    {
        public String url;
        
        public DB() {
            this.url = "";
        }
        
        public String getHelp() {
            return "";
        }
    }
    
    public static class MYSQL extends DB
    {
        @Override
        public String getHelp() {
            return "For MYSQL jdbc url looks like : jdbc:mysql://localhost:3306/testdb";
        }
    }
    
    public static class MSSQL extends DB
    {
        @Override
        public String getHelp() {
            return "For MSSQL jdbc url looks like : jdbc:sqlserver://10.1.10.1:1433;DatabaseName=DemoCDC";
        }
    }
}
