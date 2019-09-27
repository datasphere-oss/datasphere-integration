package com.datasphere.DBCommons;

import org.apache.log4j.*;

import java.io.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.source.lib.utils.*;
import com.datasphere.common.constants.*;
import java.security.*;
import java.util.*;

public class ConnectionUtil
{
    private static Logger logger;
    private static final String SSLCONFIG_DEFAULT_DELIMITER = ";";
    private static final String SSLCONFIG_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String SSLCONFIG_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String SSLCONFIG_STORE_TYPE_JKS = "JKS";
    private static final String SSLCONFIG_STORE_TYPE_PKCS12 = "PKCS12";
    private static final String SSLCONFIG_STORE_TYPE_SSO = "SSO";
    private static boolean oracle_pki_required;
    
    public static Properties getConnectionProperties(final Map<String, Object> properties) {
        final Property connproperties = new Property(properties);
        return getConnectionProperties(connproperties);
    }
    
    public static Properties getConnectionProperties(final Property property) {
        final Properties connProperties = new Properties();
        final Map<String, Object> properties = property.getMap();
        String sslConfig = null;
        String url = null;
        final Properties properties2 = connProperties;
        final String s = "user";
        property.getClass();
        properties2.put(s, property.getString("UserName", ""));
        final Properties properties3 = connProperties;
        final String s2 = "password";
        property.getClass();
        properties3.put(s2, property.getPassword("Password"));
        property.getClass();
        url = property.getString("ConnectionURL", "");
        connProperties.put("url", url);
        if (properties.containsKey("SSLConfig") && properties.get("SSLConfig") != null && ((String)properties.get("SSLConfig")).trim().length() > 0) {
            sslConfig = (String)properties.get("SSLConfig");
            String configDelimiter = ";";
            if (properties.containsKey("SSLConfigDelimiter") && properties.get("SSLConfigDelimiter") != null && ((String)properties.get("SSLConfigDelimiter")).length() > 0) {
                configDelimiter = (String)properties.get("SSLConfigDelimiter");
            }
            final String secureProperties = sslConfig.replaceAll(configDelimiter, "\n");
            try {
                connProperties.load(new StringReader(secureProperties));
                final String trustStoreType = connProperties.getProperty("javax.net.ssl.trustStoreType");
                final String keyStoreType = connProperties.getProperty("javax.net.ssl.keyStoreType");
                ConnectionUtil.oracle_pki_required = false;
                if ("PKCS12".equalsIgnoreCase(trustStoreType) || "PKCS12".equalsIgnoreCase(keyStoreType)) {
                    ConnectionUtil.oracle_pki_required = true;
                }
                else if ("SSO".equalsIgnoreCase(trustStoreType) || "SSO".equalsIgnoreCase(keyStoreType)) {
                    ConnectionUtil.oracle_pki_required = true;
                }
                connProperties.put("url", secureConnectionString(url));
            }
            catch (IOException e) {
                ConnectionUtil.logger.error((Object)("Error loading secure configuration." + secureProperties));
            }
        }
        return connProperties;
    }
    
    private static String secureConnectionString(final String connectionUrl) {
        final StringBuilder secureConnection = new StringBuilder();
        final String dbType = Utils.getDBType(connectionUrl);
        if (Constant.ORACLE_TYPE.equalsIgnoreCase(dbType)) {
            if (ConnectionUtil.oracle_pki_required) {
                try {
                    final Class securityProvider = Class.forName("oracle.security.pki.OraclePKIProvider");
                    Security.insertProviderAt((Provider)securityProvider.newInstance(), 3);
                }
                catch (Exception exception) {
                    throw new RuntimeException("Oracle PKI Jar is not present in the classpath, please add the Jar and restart the Server.", exception);
                }
            }
            final int atIndex = connectionUrl.indexOf("@");
            final String[] connHostPortService = connectionUrl.substring(atIndex + 1).split(":");
            String connService = null;
            String connPort = null;
            if (connHostPortService.length == 2) {
                final String[] tmpService = connHostPortService[1].split("/");
                connPort = tmpService[0];
                connService = tmpService[1];
            }
            else {
                connPort = connHostPortService[1];
                connService = connHostPortService[2];
            }
            secureConnection.append("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=");
            secureConnection.append(connHostPortService[0]);
            secureConnection.append(")(PORT=").append(connPort).append("))(CONNECT_DATA=(SERVICE_NAME=");
            secureConnection.append(connService).append(")))");
        }
        return secureConnection.toString();
    }
    
    static {
        ConnectionUtil.logger = Logger.getLogger((Class)ConnectionUtil.class);
        ConnectionUtil.oracle_pki_required = false;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
