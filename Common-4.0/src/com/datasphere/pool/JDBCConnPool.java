package com.datasphere.pool;

import org.apache.log4j.*;
import java.sql.*;

public class JDBCConnPool extends DefaultDBResourcePool<Connection>
{
    public static final Logger logger;
    
    public JDBCConnPool() {
    }
    
    public JDBCConnPool(final int max) {
        super(max);
    }
    
    @Override
    public Connection createResource() throws ResourceException {
        if (JDBCConnPool.logger.isInfoEnabled()) {
            JDBCConnPool.logger.info((Object)("establishing a connection: " + this.url + ", " + this.username));
        }
        Connection conn = null;
        try {
            if (this.classLoader == null) {
                this.classLoader = Thread.currentThread().getContextClassLoader();
            }
            this.classLoader.loadClass(this.drivername);
            conn = DriverManager.getConnection(this.url, this.username, this.password);
        }
        catch (ClassNotFoundException cnx) {
            throw new ResourceException("driver class not found " + this.drivername, cnx);
        }
        catch (SQLException sx) {
            throw new ResourceException("failed to get connection: " + this.url, sx);
        }
        return conn;
    }
    
    @Override
    public boolean closeResource(final Connection conn) throws ResourceException {
        if (JDBCConnPool.logger.isInfoEnabled()) {
            JDBCConnPool.logger.info((Object)"closing a connection");
        }
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (SQLException sx) {
            JDBCConnPool.logger.error((Object)"error closing connection", (Throwable)sx);
            throw new ResourceException("error closing connection", sx);
        }
        return true;
    }
    
    static {
        logger = Logger.getLogger((Class)JDBCConnPool.class);
    }
}
