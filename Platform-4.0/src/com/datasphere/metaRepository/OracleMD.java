package com.datasphere.metaRepository;

import org.apache.log4j.*;
import java.math.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import java.io.*;

public class OracleMD implements MetaDataDbProvider
{
    private static Logger logger;
    
    @Override
    public String getJDBCDriver() {
        return "oracle.jdbc.OracleDriver";
    }
    
    @Override
    public Object getIntegerValueFromData(final Object dataValue) {
        try {
            assert dataValue instanceof BigDecimal;
            final Integer data = ((BigDecimal)dataValue).intValueExact();
            return EntityType.forObject(data);
        }
        catch (AssertionError e) {
            OracleMD.logger.error((Object)("Expecting Big Decimal instance type, but received some other type" + e));
            System.exit(1);
            return null;
        }
    }
    
    @Override
    public String askMetaDataRepositoryLocation(final String default_db_location) {
        return "Enter Metadata Repository URL: ";
    }
    
    @Override
    public String askDefaultDBLocationAppendedWithPort(final String default_db_location) {
        final String[] ip_address = default_db_location.split(":");
        final String port = "1521";
        final String service = "orcl";
        return ip_address[0] + ":" + port + ":" + service;
    }
    
    @Override
    public String askDBUserName(final String default_db_Uname) {
        try {
            final String db_Uname = ConsoleReader.readLine("Enter DB User name [default hd] ( Press Enter/Return to default) : ");
            return db_Uname;
        }
        catch (IOException e) {
            OracleMD.logger.error((Object)("Could not get Metadata Repository user details beacause of IO problem, " + e.getMessage()));
            return null;
        }
    }
    
    @Override
    public String askDBPassword(final String default_db_password) {
        try {
            final String db_password = ConsoleReader.readPassword("Enter DB password [default hd]( Press Enter/Return to default) : ");
            return db_password;
        }
        catch (IOException e) {
            OracleMD.logger.error((Object)("Could not get Metadata Repository password beacause of IO problem, " + e.getMessage()));
            return null;
        }
    }
    
    @Override
    public String askDBName(final String default_db_name) {
        return default_db_name;
    }
    
    @Override
    public void setJDBCDriver() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver", false, ClassLoader.getSystemClassLoader()).newInstance();
        }
        catch (ClassNotFoundException e) {
            OracleMD.logger.error((Object)("Oracle JDBC jar may be missing; if missing add it to HD/libs/ path and restart the server: " + e));
        }
        catch (InstantiationException e2) {
            OracleMD.logger.error((Object)("JDBC driver instantiation failed, please check: " + e2));
        }
        catch (IllegalAccessException e3) {
            OracleMD.logger.error((Object)("Currently executing method does not have access to instantiate the jdbc driver: " + e3));
        }
    }
    
    @Override
    public String getJDBCURL(final String metaDataRepositoryLocation, final String metaDataRepositoryName, final String metaDataRepositoryUname, final String metaDataRepositoryPass) {
        return metaDataRepositoryLocation;
    }
    
    static {
        OracleMD.logger = Logger.getLogger((Class)OracleMD.class);
    }
}
