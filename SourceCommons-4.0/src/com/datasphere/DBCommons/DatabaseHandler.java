package com.datasphere.DBCommons;

import org.apache.log4j.*;
import java.sql.*;
import java.util.*;

public class DatabaseHandler
{
    private String databaseType;
    private int mySqlConnectionId;
    private int fetchSize;
    private Map<String, Object> properties;
    private Connection connection;
    private static Logger logger;
    
    DatabaseHandler(final Map<String, Object> properties, final Connection connection, final int fetchSize) throws Exception {
        this.mySqlConnectionId = -1;
        this.properties = properties;
        this.connection = connection;
        this.fetchSize = fetchSize;
        this.init();
    }
    
    private void init() {
        if (this.properties.containsKey("DatabaseProviderType") && this.properties.get("DatabaseProviderType") != null && !this.properties.get("DatabaseProviderType").toString().trim().isEmpty()) {
            this.databaseType = this.properties.get("DatabaseProviderType").toString().trim();
        }
        else {
            this.databaseType = "default";
        }
        try {
            int dbmatch = 0;
            final DatabaseMetaData md = this.connection.getMetaData();
            String dbname = md.getDatabaseProductName().toLowerCase();
            for (final DataBaseName databaseName : DataBaseName.values()) {
                if (databaseName.toString().equalsIgnoreCase(dbname)) {
                    dbmatch = 1;
                    break;
                }
            }
            if (dbmatch == 0 && !this.databaseType.equalsIgnoreCase("default")) {
                dbname = this.databaseType;
            }
            if (dbname != null) {
                this.databaseType = dbname;
                final String s = dbname;
                switch (s) {
                    case "enterprisedb":
                    case "postgresql": {
                        this.connection.setAutoCommit(false);
                        if (DatabaseHandler.logger.isInfoEnabled()) {
                            DatabaseHandler.logger.info((Object)"for enterprisedb and postgresql setAutoCommit is set to false");
                        }
                        break;
                    }
                    case "mysql": {
                        this.fetchSize = Integer.MIN_VALUE;
                        if (DatabaseHandler.logger.isInfoEnabled()) {
                            DatabaseHandler.logger.info((Object)"for mysql fetchSize is set to -2147483648");
                        }
                        try {
                            final Statement connectionIdStatement = this.connection.createStatement();
                            final ResultSet connectionIdresultset = connectionIdStatement.executeQuery("SELECT CONNECTION_ID()");
                            while (connectionIdresultset.next()) {
                                this.mySqlConnectionId = connectionIdresultset.getInt("CONNECTION_ID()");
                            }
                            connectionIdresultset.close();
                            connectionIdStatement.close();
                            return;
                        }
                        catch (SQLException e) {
                            final Exception exception = new Exception(e.toString(), e);
                            throw exception;
                        }
                    }
                }
                if (DatabaseHandler.logger.isInfoEnabled()) {
                    DatabaseHandler.logger.info((Object)("Database product name is -Default " + dbname));
                }
            }
        }
        catch (Exception e2) {
            DatabaseHandler.logger.error((Object)("Problem in retrieving metadata from database. " + e2), (Throwable)e2);
        }
    }
    
    public void close() throws Exception {
        final Properties connProperties = ConnectionUtil.getConnectionProperties(this.properties);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(connProperties.getProperty("url"), connProperties);
            statement = connection.createStatement();
            statement.executeQuery("kill " + this.mySqlConnectionId);
            statement.close();
            connection.close();
            if (DatabaseHandler.logger.isDebugEnabled()) {
                DatabaseHandler.logger.debug((Object)("killed the pid " + this.mySqlConnectionId));
            }
        }
        catch (SQLException e) {
            final Exception exception = new Exception(e.toString(), e);
            throw exception;
        }
        catch (Exception e2) {
            final Exception exception = new Exception(e2.toString(), e2);
            throw exception;
        }
        finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            }
            catch (Exception e3) {
                DatabaseHandler.logger.error((Object)e3.toString());
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            }
            catch (Exception e3) {
                DatabaseHandler.logger.error((Object)e3.toString());
            }
        }
    }
    
    public int getFetchSize() {
        return this.fetchSize;
    }
    
    public String getDatabaseType() {
        return this.databaseType;
    }
    
    static {
        DatabaseHandler.logger = Logger.getLogger((Class)DatabaseHandler.class);
    }
    
    public enum DataBaseName
    {
        mysql, 
        mssql, 
        oracle, 
        postgresql, 
        enterprisedb;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
