package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import java.io.*;

public class PostgresMD implements MetaDataDbProvider
{
    private static Logger logger;
    
    @Override
    public void setJDBCDriver() {
        try {
            Class.forName("org.postgresql.Driver").newInstance();
        }
        catch (InstantiationException e) {
            PostgresMD.logger.warn((Object)("Unable to create db connection : " + e.getMessage()));
        }
        catch (IllegalAccessException e2) {
            PostgresMD.logger.warn((Object)("Unable to create db connection : " + e2.getMessage()));
        }
        catch (ClassNotFoundException e3) {
            PostgresMD.logger.warn((Object)("Unable to create db connection : " + e3.getMessage()));
        }
    }
    
    @Override
    public String getJDBCURL(final String metaDataRepositoryLocation, final String metaDataRepositoryName, final String metaDataRepositoryUname, final String metaDataRepositoryPass) {
        return "jdbc:postgresql://" + metaDataRepositoryLocation + "?user=" + metaDataRepositoryUname + "&password=" + metaDataRepositoryPass;
    }
    
    @Override
    public String getJDBCDriver() {
        return "org.postgresql.Driver";
    }
    
    @Override
    public Object getIntegerValueFromData(final Object dataValue) {
        return EntityType.forObject(dataValue);
    }
    
    @Override
    public String askMetaDataRepositoryLocation(final String default_db_location) {
        final String[] ip_address = default_db_location.split(":");
        final String port = "5432";
        final String database = "hdrepos";
        return "Enter Metadata Repository Location [Format = ipAddress:port/myDb] [default " + ip_address[0] + ":" + port + "/" + database + " (Press Enter/Return to default)] : ";
    }
    
    @Override
    public String askDefaultDBLocationAppendedWithPort(final String default_db_location) {
        final String[] ip_address = default_db_location.split(":");
        final String port = "5432";
        final String database = "hdrepos";
        return ip_address[0] + ":" + port + "/" + database;
    }
    
    @Override
    public String askDBUserName(final String default_db_Uname) {
        try {
            final String db_Uname = ConsoleReader.readLine("Enter DB User name [default hd] ( Press Enter/Return to default) : ");
            return db_Uname;
        }
        catch (IOException e) {
            PostgresMD.logger.error((Object)("Could not get Metadata Repository user details beacause of IO problem, " + e.getMessage()));
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
            PostgresMD.logger.error((Object)("Could not get Metadata Repository password beacause of IO problem, " + e.getMessage()));
            return null;
        }
    }
    
    @Override
    public String askDBName(final String default_db_name) {
        String db_name = null;
        try {
            db_name = ConsoleReader.readLine("Enter DB name [default hdrepos] ( Press Enter/Return to default) : ");
            if (db_name == null || db_name.isEmpty()) {
                db_name = default_db_name;
            }
        }
        catch (IOException e) {
            PostgresMD.logger.error((Object)("Could not get Metadata Repository Data base name beacause of IO problem, " + e.getMessage()));
        }
        return db_name;
    }
    
    static {
        PostgresMD.logger = Logger.getLogger((Class)PostgresMD.class);
    }
}
