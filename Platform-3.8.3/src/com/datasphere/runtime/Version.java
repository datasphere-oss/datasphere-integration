package com.datasphere.runtime;

import org.apache.log4j.*;
import java.util.*;
import java.io.*;
import com.datasphere.metaRepository.*;
import javax.persistence.*;

public final class Version
{
    private static Logger logger;
    private static final String BUILD_PROPERTIES = "build.properties";
    private static Properties properties;
    public static final String NOT_APPLICABLE = "n/a";
    private static String mdVersion;
    
    private static String getPropertyValue(final String propertyName, final String defaultValue) {
        if (Version.properties == null) {
            Version.properties = new Properties();
            final InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(getBuildPropFileName());
            if (inStream != null) {
                try {
                    Version.properties.load(inStream);
                    inStream.close();
                }
                catch (IOException e) {
                    final Logger logger = Logger.getLogger((Class)Version.class);
                    logger.error((Object)e);
                    return null;
                }
            }
        }
        final String value = Version.properties.getProperty(propertyName, defaultValue);
        return value;
    }
    
    public static String getPOMVersionString() {
        return getPropertyValue("pom_version", "1.0.0-SNAPSHOT");
    }
    
    private static String getBuildRevisionString() {
        return getPropertyValue("scm_revision", "Development Version - Not for Release");
    }
    
    public static String getVersionString() {
        return "Version " + getPOMVersionString() + " (" + getBuildRevisionString() + ")";
    }
    
    public static String getMetaDataVersion() {
        if (Version.mdVersion == null && Server.persistenceIsEnabled()) {
            final EntityManager entityManager = MetaDataDBOps.getManager();
            final String qry = "select version from ".concat(MetaDataDBOps.DBUname).concat(".").concat("hd_version");
            try {
                final Query query = entityManager.createNativeQuery(qry);
                Version.mdVersion = (String)query.getSingleResult();
            }
            catch (Exception ex) {
                Version.logger.error((Object)("error executing " + qry), (Throwable)ex);
            }
            if (Version.logger.isInfoEnabled()) {
                Version.logger.info((Object)("MetaData version : " + Version.mdVersion));
            }
        }
        return Version.mdVersion;
    }
    
    public static boolean isConnectedToCompatibleMetaData() {
        boolean result = false;
        final String mdv = getMetaDataVersion();
        final String codeVersion = getPOMVersionString();
        if (Version.logger.isDebugEnabled()) {
            Version.logger.debug((Object)("codeVersion:" + codeVersion + ", md version:" + mdv));
        }
        if (codeVersion.equals("1.0.0-SNAPSHOT")) {
            return true;
        }
        if (mdv == null || mdv.isEmpty()) {
            return result;
        }
        final String[] parts1 = mdv.split("\\.");
        String major = "";
        String minor = "";
        final String patch = "";
        if (parts1.length <= 1) {
            return result;
        }
        major = parts1[0];
        minor = parts1[1];
        String majorc = "";
        String minorc = "";
        final String patchc = "";
        final String[] parts2 = codeVersion.split("\\.");
        if (parts2.length > 1) {
            majorc = parts2[0];
            minorc = parts2[1];
            if (majorc.equalsIgnoreCase(major) && (minorc.equalsIgnoreCase(minor) || Integer.parseInt(minorc) > Integer.parseInt(minor))) {
                result = true;
            }
            return result;
        }
        return result;
    }
    
    public static String getBuildPropFileName() {
        return "build.properties";
    }
    
    static {
        Version.logger = Logger.getLogger((Class)Version.class);
        Version.properties = null;
        Version.mdVersion = null;
    }
}
