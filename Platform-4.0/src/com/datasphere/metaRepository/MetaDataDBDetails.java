package com.datasphere.metaRepository;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.security.*;
import java.security.*;
import java.io.*;
import com.datasphere.runtime.*;
import java.sql.*;

public class MetaDataDBDetails
{
    String location;
    String name;
    String UName;
    String password;
    Connection conn;
    private static Logger logger;
    public final String default_db_port = "1527";
    public final String colon = ":";
    public final String default_db_location;
    public static final String default_db_name = "hdrepos";
    public static final String default_db_Uname = "hd";
    public static final String default_db_password = "w@ct10n";
    private boolean firstTime;
    
    public MetaDataDBDetails() {
        this.location = null;
        this.name = null;
        this.UName = null;
        this.password = null;
        this.conn = null;
        this.default_db_location = HazelcastSingleton.getBindingInterface().concat(":").concat("1527");
        this.firstTime = true;
    }
    
    public Map<String, String> takeDBDetails() throws IOException {
        for (boolean gotDBDetailsRight = false; !gotDBDetailsRight; gotDBDetailsRight = this.askDBDetails()) {}
        final Map<String, String> dbDetails = new HashMap<String, String>();
        dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation(), this.location);
        dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname(), this.name);
        dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname(), this.UName);
        dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), this.password);
        return dbDetails;
    }
    
    public static String encryptDBPassword(final String toBeEncrypted, final String saltString) throws GeneralSecurityException, UnsupportedEncodingException {
        final Random r = new Random(saltString.hashCode());
        final byte[] salt = new byte[8];
        r.nextBytes(salt);
        return HSecurityManager.encrypt(toBeEncrypted, salt);
    }
    
    public static String decryptDBPassword(final String toBeDecrypted, final String saltString) throws Exception {
        final Random r = new Random(saltString.hashCode());
        final byte[] salt = new byte[8];
        r.nextBytes(salt);
        return HSecurityManager.decrypt(toBeDecrypted, salt);
    }
    
    public boolean askDBDetails() {
        final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
        try {
            final String mdLocation = System.getProperty("com.striim.config.metadataLocation");
            if (mdLocation == null || mdLocation.isEmpty()) {
                System.out.println("Required property \"Metadata Repository Location\" is undefined");
                this.location = ConsoleReader.readLine(metaDataDbProvider.askMetaDataRepositoryLocation(this.default_db_location));
            }
            else {
                this.location = mdLocation;
            }
            this.name = metaDataDbProvider.askDBName("hdrepos");
            final String passedUserName = System.getProperty("com.striim.config.metadataUserName");
            if (passedUserName == null || passedUserName.isEmpty()) {
                this.UName = metaDataDbProvider.askDBUserName("hd");
            }
            else {
                this.UName = passedUserName;
            }
            if (metaDataDbProvider instanceof DerbyMD && this.firstTime) {
                this.password = "w@ct10n";
                this.firstTime = false;
            }
            else {
                final String passedPassword = System.getProperty("com.striim.config.metadataPassword");
                if (passedPassword == null || passedPassword.isEmpty()) {
                    this.password = metaDataDbProvider.askDBPassword("w@ct10n");
                }
                else {
                    this.password = passedPassword;
                }
            }
        }
        catch (IOException e) {
            MetaDataDBDetails.logger.error((Object)("Could not get Metadata Repository Details beacause of IO problem, " + e.getMessage()));
        }
        if (this.location == null || this.location.isEmpty()) {
            this.location = metaDataDbProvider.askDefaultDBLocationAppendedWithPort(this.default_db_location);
        }
        if (this.name == null || this.name.isEmpty()) {
            this.name = "hdrepos";
        }
        if (this.UName == null || this.UName.isEmpty()) {
            this.UName = "hd";
        }
        if (this.password == null || this.password.isEmpty()) {
            this.password = "w@ct10n";
        }
        boolean b;
        try {
            b = this.createConnection(this.location, this.name, this.UName, this.password);
        }
        catch (SQLException e2) {
            MetaDataDBDetails.logger.error((Object)e2.getMessage(), (Throwable)e2);
            b = false;
        }
        finally {
            this.closeConnection();
        }
        return b;
    }
    
    public boolean createConnection(final String metaDataRepositoryLocation, final String metaDataRepositoryName, final String metaDataRepositoryUname, final String metaDataRepositoryPass) throws SQLException {
        final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
        final String dbURL = metaDataDbProvider.getJDBCURL(metaDataRepositoryLocation, metaDataRepositoryName, metaDataRepositoryUname, metaDataRepositoryPass);
        metaDataDbProvider.setJDBCDriver();
        boolean connected;
        try {
            this.conn = DriverManager.getConnection(dbURL, metaDataRepositoryUname, metaDataRepositoryPass);
            connected = true;
        }
        catch (Exception e) {
            throw e;
        }
        return connected;
    }
    
    public boolean isConnectionAlive(final String metaDataRepositoryLocation, final String metaDataRepositoryName, final String metaDataRepositoryUname, final String metaDataRepositoryPass) {
        boolean result = false;
        try {
            result = this.createConnection(metaDataRepositoryLocation, metaDataRepositoryName, metaDataRepositoryUname, metaDataRepositoryPass);
        }
        catch (SQLException e) {
            MetaDataDBDetails.logger.error((Object)e.getMessage(), (Throwable)e);
        }
        finally {
            this.closeConnection();
        }
        return result;
    }
    
    public void closeConnection() {
        if (this.conn != null) {
            try {
                this.conn.close();
                if (MetaDataDBDetails.logger.isDebugEnabled()) {
                    MetaDataDBDetails.logger.debug((Object)"Test connection closed");
                }
            }
            catch (SQLException e) {
                MetaDataDBDetails.logger.error((Object)("Connection not closed : " + e.getMessage()), (Throwable)e);
            }
        }
    }
    
    static {
        MetaDataDBDetails.logger = Logger.getLogger((Class)MetaDataDBDetails.class);
    }
}
