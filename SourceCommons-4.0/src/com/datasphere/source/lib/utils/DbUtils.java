package com.datasphere.source.lib.utils;

import java.sql.*;
import org.apache.ddlutils.*;

public class DbUtils
{
    public static dbtype getDbType(final Connection conn) throws SQLException {
        final DatabaseMetaData metaData = conn.getMetaData();
        return getDbType(metaData.getDriverName(), metaData.getURL());
    }
    
    public static dbtype getDbType(final DatabaseMetaData metaData) throws SQLException {
        return getDbType(metaData.getDriverName(), metaData.getURL());
    }
    
    public static dbtype getDbType(final String driverName, final String connUrl) {
        final String str = new PlatformUtils().determineDatabaseType(driverName, connUrl);
        if (str.equalsIgnoreCase("MsSql")) {
            return dbtype.MSSQL;
        }
        if (str.equalsIgnoreCase("Oracle")) {
            return dbtype.ORACLE;
        }
        if (str.equalsIgnoreCase("MySQL")) {
            return dbtype.MYSQL;
        }
        if (str.equalsIgnoreCase("PostgreSql")) {
            return dbtype.POSTGRE;
        }
        if (str.equalsIgnoreCase("Derby")) {
            return dbtype.DERBY;
        }
        return dbtype.UNKNOWN;
    }
    
    public enum dbtype
    {
        ORACLE, 
        MSSQL, 
        MYSQL, 
        DERBY, 
        POSTGRE, 
        UNKNOWN;
    }
}
