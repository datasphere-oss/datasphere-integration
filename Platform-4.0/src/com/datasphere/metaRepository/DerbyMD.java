package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import java.io.*;

public class DerbyMD implements MetaDataDbProvider
{
    private static Logger logger;
    
    @Override
    public String getJDBCDriver() {
        return "org.apache.derby.jdbc.ClientDriver";
    }
    
    @Override
    public Object getIntegerValueFromData(final Object dataValue) {
        return EntityType.forObject(dataValue);
    }
    
    @Override
    public String askMetaDataRepositoryLocation(final String default_db_location) {
        return "Enter Metadata Repository Location [Format = IPAddress:port] [default " + default_db_location + " (Press Enter/Return to default)] : ";
    }
    
    @Override
    public String askDefaultDBLocationAppendedWithPort(final String default_db_location) {
        return default_db_location;
    }
    
    @Override
    public String askDBUserName(final String default_db_Uname) {
        return default_db_Uname;
    }
    
    @Override
    public String askDBPassword(final String default_db_password) {
        try {
            final String db_password = ConsoleReader.readPassword("Enter DB password: ");
            return db_password.isEmpty() ? default_db_password : db_password;
        }
        catch (IOException e) {
            DerbyMD.logger.error((Object)("Could not get Metadata Repository password beacause of IO problem, " + e.getMessage()));
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
            Class.forName("org.apache.derby.jdbc.ClientDriver", false, ClassLoader.getSystemClassLoader()).newInstance();
        }
        catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex2) {
            DerbyMD.logger.warn((Object)("Unable to create db connection : " + ex2.getMessage()));
        }
    }
    
    @Override
    public String getJDBCURL(final String metaDataRepositoryLocation, final String metaDataRepositoryName, final String metaDataRepositoryUname, final String metaDataRepositoryPass) {
        return "jdbc:derby://" + metaDataRepositoryLocation + "/" + metaDataRepositoryName;
    }
    
    static {
        DerbyMD.logger = Logger.getLogger((Class)DerbyMD.class);
    }
}
