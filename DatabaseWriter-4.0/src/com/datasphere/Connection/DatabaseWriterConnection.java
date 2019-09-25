package com.datasphere.Connection;

import org.slf4j.*;

import com.datasphere.utility.*;
import com.datasphere.databasewriter.DatabaseWriterProcessEvent;
import com.datasphere.exception.*;
import com.datasphere.common.constants.*;

import java.sql.*;
import com.datasphere.source.lib.utils.*;

public class DatabaseWriterConnection
{
    protected Connection connection;
    private Logger logger;
    private String databaseName;
    private String targetDatabaseType;
    protected String schema;
    protected String catalog;
    protected String dbUrl;
    protected String dbUser;
    protected String dbPasswd;
    
    protected DatabaseWriterConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        this.connection = null;
        this.logger = LoggerFactory.getLogger((Class)DatabaseWriterConnection.class);
        this.connection = null;
        this.databaseName = null;
        this.connect(this.dbUrl = dbUrl, this.dbUser = dbUser, this.dbPasswd = dbPasswd);
        this.initialize();
    }
    
    protected void initialize() throws DatabaseWriterException {
        this.schema = this.getSchema(this.connection);
        this.catalog = this.getCatalog(this.connection);
    }
    
    public String getDatabaseName() {
        return this.databaseName;
    }
    
    public String schema() {
        return this.schema;
    }
    
    public String catalog() {
        return this.catalog;
    }
    
    protected String getSchema(final Connection conn) throws DatabaseWriterException {
        return null;
    }
    
    protected String getCatalog(final Connection conn) throws DatabaseWriterException {
        return null;
    }
    
    public String targetType() {
        return this.targetDatabaseType;
    }
    
    public void connect(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        if (dbUrl.contains(":t4sqlmx:")) {
            try {
                Class.forName("com.tandem.t4jdbc.SQLMXDriver");
            }
            catch (ClassNotFoundException e) {
                final DatabaseWriterException exception = new DatabaseWriterException(Error.FAILURE_CONNECTING_TO_DATABASE, " with url : " + dbUrl + " username : " + dbUser + "\n Cause : " + e.getCause() + ";Message : " + e.getMessage());
                this.logger.error(exception.getMessage());
                throw exception;
            }
        }
        final String dbType = DBUtils.getDbTypeByUrl(dbUrl);
        if (dbType.isEmpty()) {
            throw new DatabaseWriterException(Error.INVALID_CONNECTIONURL_FORMAT, "Invalid URL format {" + dbUrl + "}");
        }
        this.targetDatabaseType = dbType;
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Connecting to Database with url:" + dbUrl + " username : " + dbUser);
        }
        try {
            if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(dbType) || Constant.GREENPLUM_TYPE.equalsIgnoreCase(dbType)) {
                String url = dbUrl;
                try {
                    if (url.indexOf("stringtype=unspecified") < 0) {
                        if (url.indexOf("?") < 0) {
                            url += "?stringtype=unspecified";
                        }
                        else {
                            url += "stringtype=unspecified";
                        }
                    }
                    Class.forName("org.postgresql.Driver");
                    this.connection = DriverManager.getConnection(url, dbUser, dbPasswd);
                }
                catch (Exception e4) {
                    if (url.indexOf("stringtype=unspecified") < 0) {
                        if (url.indexOf("?") < 0) {
                            url += "?stringtype=unspecified";
                        }
                        else {
                            url += "stringtype=unspecified";
                        }
                    }
                    this.connection = DriverManager.getConnection(url, dbUser, dbPasswd);
                }
            }
            else {
                String url = dbUrl;
                if (url.startsWith("jdbc:ultradb:")) {
                    Class.forName("com.ultracloud.ultradb.jdbc.UltraDBDriver");
                }
                else if (url.startsWith("jdbc:gbase:")) {
                    Class.forName("com.gbase.jdbc.Driver");
                }
                else if (url.startsWith("jdbc:postgresql:")) {
                    Class.forName("org.postgresql.Driver");
                    if (url.indexOf("stringtype") < 0) {
                        if (url.indexOf("?") < 0) {
                            url += "?stringtype=unspecified";
                        }
                        else {
                            url += "stringtype=unspecified";
                        }
                    }
                }
                else if (url.startsWith("jdbc:dm:")) {
                    Class.forName("dm.jdbc.driver.DmDriver");
                }
                else if (url.startsWith("jdbc:informix-sqli:")) {
                    Class.forName("com.informix.jdbc.IfxDriver");
                }
                else if (url.startsWith("jdbc:highgo:")) {
                    Class.forName("com.highgo.jdbc.Driver");
                }
                else if (url.startsWith("jdbc:mysql:")) {
                    if (url.indexOf("useCursorFetch") < 0) {
                        if (url.indexOf("?") < 0) {
                            url += "?useCursorFetch=true";
                        }
                        else {
                            url += "&useCursorFetch=true";
                        }
                    }
                    if (url.indexOf("rewriteBatchedStatements") < 0) {
	                      if (url.indexOf("?") < 0) {
	                          url += "?rewriteBatchedStatements=true";
	                      }
	                      else {
	                          url += "&rewriteBatchedStatements=true";
	                      }
	                  }
	                  if (url.indexOf("autoReconnect") < 0) {
	                      if (url.indexOf("?") < 0) {
	                          url += "?autoReconnect=true";
	                      }
	                      else {
	                          url += "&autoReconnect=true";
	                      }
	                  }
	                  if (url.indexOf("failOverReadOnly") < 0) {
	                      if (url.indexOf("?") < 0) {
	                          url += "?failOverReadOnly=false";
	                      }
	                      else {
	                          url += "&failOverReadOnly=false";
	                      }
	                  }
	                  if (url.indexOf("maxReconnects") < 0) {
	                      if (url.indexOf("?") < 0) {
	                          url += "?maxReconnects=30";
	                      }
	                      else {
	                          url += "&maxReconnects=30";
	                      }
	                  }
	                  if (url.indexOf("initialTimeout") < 0) {
	                      if (url.indexOf("?") < 0) {
	                          url += "?initialTimeout=30";
	                      }
	                      else {
	                          url += "&initialTimeout=30";
	                      }
	                  }
                    try {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                    }
                    catch (ClassNotFoundException ex) {
                        Class.forName("com.mysql.jdbc.Driver");
                    }
                }
                else if (url.startsWith("jdbc:oracle:")) {
                    Class.forName("oracle.jdbc.OracleDriver");
                }
                else if (url.startsWith("jdbc:edb:")) {
                    Class.forName("com.edb.Driver");
                }
                else if (url.startsWith("jdbc:kingbase:")) {
                    Class.forName("com.kingbase.Driver");
                }
                else if (url.startsWith("jdbc:db2:")) {
                    Class.forName("com.ibm.db2.jdbc.net.DB2Driver");
                }
                else if (url.startsWith("jdbc:edb:")) {
                    Class.forName("com.edb.Driver");
                }
                else if (url.startsWith("jdbc:teradata:")) {
                    Class.forName("com.teradata.jdbc.TeraDriver");
                }
                else if (url.startsWith("jdbc:t4jdbc:")) {
                    Class.forName("org.trafodion.jdbc.t4.T4Driver");
                }
                this.connection = DriverManager.getConnection(url, dbUser, dbPasswd);
            }
        }
        catch (SQLException e2) {
            final DatabaseWriterException exception2 = new DatabaseWriterException(Error.FAILURE_CONNECTING_TO_DATABASE, " with url : " + dbUrl + " username : " + dbUser + " \n ErrorCode : " + e2.getErrorCode() + ";SQLCode : " + e2.getSQLState() + ";SQL Message : " + e2.getMessage());
            this.logger.error(exception2.getMessage());
            throw exception2;
        }
        catch (Exception e3) {
            final DatabaseWriterException exception2 = new DatabaseWriterException(Error.FAILURE_CONNECTING_TO_DATABASE, " with url : " + dbUrl + " username : " + dbUser + " \n Cause : " + e3.getCause() + ";Message : " + e3.getMessage());
            this.logger.error(exception2.getMessage());
            throw exception2;
        }
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Connected to Database with url:" + dbUrl);
        }
    }
    
    public DatabaseWriterConnection reconnect() throws DatabaseWriterException {
        return new DatabaseWriterConnection(this.dbUrl, this.dbUser, this.dbPasswd);
    }
    
    public Connection getConnection() {
        return this.connection;
    }
    
    public void disconnect() throws DatabaseWriterException {
        if (this.connection == null) {
            this.logger.info("Connection is already closed");
        }
        else {
            try {
                this.connection.close();
            }
            catch (SQLException e) {
                final DatabaseWriterException exception = new DatabaseWriterException(Error.FAILURE_DISCONNECTING_FROM_DATABASE, " \n ErrorCode : " + e.getErrorCode() + ";SQLCode : " + e.getSQLState() + ";SQL Message : " + e.getMessage());
                this.logger.error(exception.getMessage());
                throw exception;
            }
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Closed database connection sucessfully");
            }
        }
    }
    
    public String fullyQualifiedName(final String table) {
        String fqn = "";
        final String[] parts = table.split("\\.");
        if (parts.length < 3 && this.catalog != null && !this.catalog.isEmpty()) {
            fqn = this.catalog;
        }
        if (parts.length < 2 && this.schema != null && !this.schema.isEmpty()) {
            if (fqn.isEmpty()) {
                fqn = this.schema;
            }
            else {
                fqn = String.valueOf(fqn) + "." + this.schema;
            }
        }
        if (fqn.isEmpty()) {
            fqn = table;
        }
        else {
            fqn = String.valueOf(fqn) + "." + table;
        }
        return fqn;
    }
    
    public TableNameParts getTableNameParts(final String tableName) {
        String schema = null;
        String catalog = null;
        String table = null;
        final String[] part = tableName.trim().split("\\.");
        if (part.length >= 1) {
            if (part.length >= 2) {
                if (part.length >= 3) {
                    if (part.length == 3) {
                        catalog = part[0];
                        schema = part[1];
                        table = part[2];
                    }
                }
                else {
                    schema = part[0];
                    table = part[1];
                }
            }
            else {
                table = part[0];
            }
        }
        return new TableNameParts(catalog, schema, table);
    }
    
    public static DatabaseWriterConnection getConnection(final String dbUrl, final String dbUser, final String dbPasswd) throws DatabaseWriterException {
        DatabaseWriterConnection connection = null;
        final String dbType = Utils.getDBType(dbUrl);
        if (Constant.ORACLE_TYPE.equalsIgnoreCase(dbType)) {
            connection = new OracleConnection(dbUrl, dbUser, dbPasswd);
        }
        else if (Constant.MYSQL_TYPE.equalsIgnoreCase(dbType)) {
            connection = new MySQLConnection(dbUrl, dbUser, dbPasswd);
        }
        else if (Constant.MSSQL_TYPE.equalsIgnoreCase(dbType)) {
            connection = new MSSQLConnection(dbUrl, dbUser, dbPasswd);
        }
        else if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(dbType) || Constant.EDB_TYPE.equalsIgnoreCase(dbType)) {
            connection = new PostgresConnection(dbUrl, dbUser, dbPasswd);
        }
        else {
            connection = new DatabaseWriterConnection(dbUrl, dbUser, dbPasswd);
        }
        return connection;
    }
    
    public static void main(final String[] args) {
        final String url = "jdbc:oracle:thin:@192.168.0.1:1528:helowin";
        System.out.println(DBUtils.getDbTypeByUrl(url));
        try {
            final DatabaseWriterConnection con = new DatabaseWriterConnection(url, "test", "test");
            System.out.println("----catalog:"+con.catalog);
            System.out.println("----schema:"+con.schema);
            
            final DatabaseMetaData dbMeta = con.getConnection().getMetaData();
            DatabaseWriterConnection.TableNameParts tps = con.getTableNameParts("DATALLIANCE.CHKPOINT");
            
            tps.catalog = con.getConnection().getCatalog();
            System.out.println("---tps.catalog:"+tps.catalog);
            tps.schema = con.getConnection().getSchema();
            System.out.println("---tps.schema:"+tps.schema);
            System.out.println("---tps.table:"+tps.table);
            
			ResultSet tableInfoResultSet = dbMeta.getTables(tps.catalog, tps.schema, tps.table,
					new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
			while(tableInfoResultSet.next()) {
			System.out.println(tableInfoResultSet.getString(1) + ","+tableInfoResultSet.getString(2) + ","+tableInfoResultSet.getString(3) +","+ tableInfoResultSet.getString(4) +","+ tableInfoResultSet.getString(5));
			}
			
			ResultSet columnInfoResultSet = dbMeta.getColumns(tps.catalog, tps.schema, tps.table, null);
			
			ResultSet tableKeyResultSet = dbMeta.getPrimaryKeys(tps.catalog, tps.schema, tps.table);
			con.disconnect();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public class TableNameParts
    {
        public String schema;
        public String catalog;
        public String table;
        
        public TableNameParts(final String catalog, final String schema, final String table) {
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
        }
    }
    
    class SecurityAccess
    {
        public void disopen() {
        }
    }
}
