package com.datasphere.source.lib.prop;

import org.apache.log4j.*;
import java.util.*;

import com.datasphere.security.*;
import com.datasphere.source.lib.constant.*;

public class Property
{
    private Logger logger;
    public final String USER_NAME = "UserName";
    public final String PASSWORD = "Password";
    public final String TOPIC = "Topic";
    public final String STREAMNAME = "streamName";
    public final String CONNECTION_URL = "ConnectionURL";
    private static final String REGIONNAME = "regionName";
    private static final String ACCESSKEYID = "accessKeyId";
    private static final String SECRETACCESSKEY = "secretAccessKey";
    public final String QUEUE_NAME = "queuename";
    public final String CONTEXT = "Ctx";
    public final String PROVIDER = "Provider";
    public final int MAX_BLOCK_SIZE = 10240;
    public final String ROOT_NODE = "rootnode";
    public static String COMPRESSION_TYPE;
    public static String ARCHIVE_TYPE;
    public static String CHARSET;
    public static String READER_TYPE;
    public static String IPADDRESS;
    public final String AUTHENTICATE_CLIENT = "authenticateclient";
    public final String CONNECTION_FACTORY_NAME = "connectionfactoryname";
    public static String DIRECTORY;
    public static String WILDCARD;
    public static String GROUP_PATTERN;
    public static String THREAD_POOL_SIZE;
    public static String YIELD_AFTER;
    public static String NETWORK_FILE_SYSTEM;
    public static String POLL_INTERVAL;
    public static int DEFAULT_YIELD_AFTER;
    public static String SOURCE_UUID;
    public final String MAX_CONCURRENT_CLIENTS = "maxConcurrentClient";
    public static final String AUTH_FILE_LOCATION = "authfilelocation";
    public static final String SKIP_BOM = "skipbom";
    public final int DEFAULT_MAX_CONCURRENT_CLIENTS = 5;
    public static String MESSAGE_TYPE;
    public final int DEFAILT_THREAD_POOL_SIZE = 20;
    public final String AUTHENTICATION_POLICY = "authenticationpolicy";
    public final String HADOOP_CONFIGURATION_PATH = "hadoopconfigurationpath";
    public static String ROLLOVER_STYLE;
    public static String DEFAULT_FILE_SEQUENCER;
    public String directory;
    public String outdirectory;
    public String wildcard;
    public String remoteaddress;
    public String archivedir;
    public String logtype;
    public String columndelimiterlist;
    public String rowdelimiterlist;
    public String charset;
    public String metaColumnList;
    public String[] nvprecorddelimiter;
    public String[] nvpvaluedelimiter;
    public int eofdelay;
    public int maxcolumnoffset;
    public int connectionTimeout;
    public int readTimeout;
    public int blocksize;
    public int portno;
    public int lineoffset;
    public int headerlineno;
    public int retryAttempt;
    public int threadCount;
    public char rowdelimiter;
    public char columndelimiter;
    public char returndelimiter;
    public char quotecharacter;
    public boolean nocolumndelimiter;
    public boolean trimwhitespace;
    public boolean registerRecursively;
    public boolean positionByEOF;
    public boolean skipBOM;
    public boolean authenticateClient;
    public String[] interestedcolumnList;
    public String ipaddress;
    public String userName;
    public String password;
    public String topic;
    public String queueName;
    public String context;
    public String provider;
    public String hadoopUrl;
    public String rollOverPolicy;
    public String keyStoreType;
    public String keyStore;
    public String keyStorePassword;
    public String regionName;
    public String accessKeyId;
    public String secretAccessKey;
    public String streamName;
    public String aliasConfigFile;
    public String authenticationFileLocation;
    public Map<String, Object> propMap;
    public int maxConcurrentClients;
    public boolean dontBlockOnEOF;
    public String connectionFactoryName;
    public String messageType;
    public String groupPattern;
    public int threadPoolSize;
    public String authenticationPolicy;
    public String hadoopConfigurationPath;
    
    protected char getChar(final Map<String, Object> map, final String key) {
        char c = '\0';
        final Object val = map.get(key);
        if (val instanceof Character) {
            c = (char)val;
        }
        else if (val instanceof String && ((String)val).length() > 0) {
            c = ((String)val).charAt(0);
        }
        return c;
    }
    
    protected int getInt(final Map<String, Object> map, final String key) {
        int c = 0;
        final Object val = map.get(key);
        if (val instanceof Number) {
            c = ((Number)val).intValue();
        }
        else if (val instanceof String) {
            c = Integer.parseInt((String)val);
        }
        return c;
    }
    
    protected boolean getBoolean(final Map<String, Object> map, final String key) {
        boolean bool = false;
        final Object val = map.get(key);
        if (val instanceof Boolean) {
            bool = (boolean)val;
        }
        else if (val instanceof String && (((String)val).equalsIgnoreCase("true") || ((String)val).equalsIgnoreCase("yes"))) {
            bool = true;
        }
        return bool;
    }
    
    public Property(final Map<String, Object> map) {
        this.logger = Logger.getLogger((Class)Property.class);
        this.dontBlockOnEOF = false;
        (this.propMap = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER)).putAll(map);
        final Map<String, Object> mp = this.propMap;
        Object rowDelimiterValue = null;
        Object delimiterValue = null;
        this.directory = (String)mp.get(Property.DIRECTORY);
        this.outdirectory = (String)mp.get("outdir");
        this.charset = this.getString(Property.CHARSET, null);
        if (mp.get("blocksize") == null) {
            this.blocksize = 64;
        }
        else {
            this.blocksize = this.getInt(mp, "blocksize");
            if (this.blocksize < 0) {
                this.blocksize = -this.blocksize;
            }
            else if (this.blocksize == 0) {
                this.blocksize = 64;
            }
            if (this.blocksize > 10240) {
                this.logger.warn((Object)("Block size {" + this.blocksize * 1024 + "} quite high, it could increase server memory usage"));
            }
        }
        if (mp.get("nocolumndelimiter") == null) {
            this.nocolumndelimiter = false;
        }
        else {
            this.nocolumndelimiter = this.getBoolean(mp, "nocolumndelimiter");
        }
        if (mp.get("rowdelimiter") == null) {
            this.rowdelimiter = '\n';
        }
        else {
            rowDelimiterValue = mp.get("rowdelimiter");
            this.rowdelimiter = this.getChar(mp, "rowdelimiter");
        }
        if (mp.get("maxcolumncount") == null) {
            this.maxcolumnoffset = Constant.MAX_COLUMN_OFFSET_DEFAULT;
        }
        else {
            this.maxcolumnoffset = this.getInt(mp, "maxcolumncount");
        }
        if (mp.get("columndelimiter") == null && !this.nocolumndelimiter) {
            this.columndelimiter = ',';
        }
        else {
            delimiterValue = mp.get("columndelimiter");
            if (this.nocolumndelimiter) {
                this.columndelimiter = '\0';
            }
            else {
                this.columndelimiter = this.getChar(mp, "columndelimiter");
            }
        }
        if (delimiterValue != null && delimiterValue instanceof String) {
            this.columndelimiterlist = (String)delimiterValue;
        }
        else {
            this.columndelimiterlist = String.valueOf(this.columndelimiter);
        }
        if (rowDelimiterValue != null && rowDelimiterValue instanceof String) {
            this.rowdelimiterlist = (String)rowDelimiterValue;
        }
        else {
            this.rowdelimiterlist = String.valueOf(this.rowdelimiter);
        }
        if (mp.get("quotecharacter") == null) {
            this.quotecharacter = '\"';
        }
        else {
            this.quotecharacter = this.getChar(mp, "quotecharacter");
        }
        if (mp.get("eofDelay") == null) {
            this.eofdelay = 1000;
        }
        else {
            this.eofdelay = this.getInt(mp, "eofDelay");
            if (this.eofdelay < 0) {
                this.eofdelay = -this.eofdelay;
            }
        }
        if (mp.get("connectionTimeout") == null) {
            this.connectionTimeout = 1000;
        }
        else {
            this.connectionTimeout = this.getInt(mp, "connectionTimeout");
            if (this.connectionTimeout < 0) {
                this.connectionTimeout = -this.connectionTimeout;
            }
        }
        if (mp.get("readTimeout") == null) {
            this.readTimeout = 1000;
        }
        else {
            this.readTimeout = this.getInt(mp, "readTimeout");
            if (this.readTimeout < 0) {
                this.readTimeout = -this.readTimeout;
            }
        }
        if (mp.get("remoteaddress") == null) {
            this.remoteaddress = Constant.REMOTE_ADDRESS_DEFAULT;
        }
        else {
            this.remoteaddress = (String)mp.get("remoteaddress");
        }
        this.ipaddress = (String)mp.get(Property.IPADDRESS);
        if (this.ipaddress == null) {
            this.ipaddress = "localhost";
        }
        if (mp.get("portno") == null) {
            this.portno = Constant.PORT_NO_DEFAULT;
        }
        else {
            this.portno = this.getInt(mp, "portno");
        }
        if (mp.get("LineNumber") != null) {
            this.lineoffset = this.getInt(mp, "LineNumber");
        }
        else {
            this.lineoffset = 0;
        }
        this.returndelimiter = '\r';
        if (mp.get("trimwhitespace") != null) {
            this.trimwhitespace = this.getBoolean(mp, "trimwhitespace");
        }
        else {
            this.trimwhitespace = false;
        }
        final String colList = (String)mp.get("columnlist");
        if (colList != null) {
            String delimiter = (String)mp.get("separator");
            if (delimiter == null) {
                delimiter = ",";
            }
            this.interestedcolumnList = colList.split(delimiter);
        }
        if (mp.get(Property.WILDCARD) != null) {
            this.wildcard = (String)mp.get(Property.WILDCARD);
        }
        if (mp.get("subdirectory") != null) {
            this.registerRecursively = this.getBoolean(mp, "subdirectory");
        }
        else {
            this.registerRecursively = false;
        }
        if (mp.get("MetaColumnList") != null) {
            this.metaColumnList = (String)mp.get("MetaColumnList");
        }
        final Map<String, Object> map2 = mp;
        this.getClass();
        if (map2.get("UserName") != null) {
            final Map<String, Object> map3 = mp;
            this.getClass();
            this.userName = (String)map3.get("UserName");
        }
        final Map<String, Object> map4 = mp;
        this.getClass();
        if (map4.get("Password") != null) {
            final Map<String, Object> map5 = mp;
            this.getClass();
            final Object passwd = map5.get("Password");
            if (passwd instanceof String) {
                this.password = (String)passwd;
            }
        }
        final Map<String, Object> map6 = mp;
        this.getClass();
        if (map6.get("Topic") != null) {
            final Map<String, Object> map7 = mp;
            this.getClass();
            this.topic = (String)map7.get("Topic");
        }
        final Map<String, Object> map8 = mp;
        this.getClass();
        if (map8.get("streamName") != null) {
            final Map<String, Object> map9 = mp;
            this.getClass();
            this.streamName = (String)map9.get("streamName");
        }
        if (mp.get("regionName") != null) {
            this.regionName = (String)mp.get("regionName");
        }
        if (mp.get("accessKeyId") != null) {
            this.accessKeyId = (String)mp.get("accessKeyId");
        }
        if (mp.get("secretAccessKey") != null) {
            final Object secretAccessKey = mp.get("secretAccessKey");
            if (secretAccessKey instanceof Password) {
                this.secretAccessKey = ((Password)secretAccessKey).getPlain();
            }
            else {
                this.secretAccessKey = (String)secretAccessKey;
            }
        }
        final Map<String, Object> map10 = mp;
        this.getClass();
        if (map10.get("queuename") != null) {
            final Map<String, Object> map11 = mp;
            this.getClass();
            this.queueName = (String)map11.get("queuename");
        }
        final Map<String, Object> map12 = mp;
        this.getClass();
        if (map12.get("Ctx") != null) {
            final Map<String, Object> map13 = mp;
            this.getClass();
            this.context = (String)map13.get("Ctx");
        }
        final Map<String, Object> map14 = mp;
        this.getClass();
        if (map14.get("Provider") != null) {
            final Map<String, Object> map15 = mp;
            this.getClass();
            this.provider = (String)map15.get("Provider");
        }
        if (mp.get("positionByEOF") != null) {
            this.positionByEOF = this.getBoolean(mp, "positionByEOF");
        }
        else {
            this.positionByEOF = true;
        }
        if (mp.get("retryAttempt") != null) {
            this.retryAttempt = this.getInt(mp, "retryAttempt");
        }
        else {
            this.retryAttempt = -1;
        }
        if (mp.get("hadoopurl") != null) {
            this.hadoopUrl = (String)mp.get("hadoopurl");
            if (!this.hadoopUrl.endsWith("/")) {
                this.hadoopUrl += "/";
            }
        }
        if (mp.get("skipbom") != null) {
            this.skipBOM = this.getBoolean(mp, "skipbom");
        }
        else {
            this.skipBOM = true;
        }
        if (mp.get("rolloverpolicy") != null) {
            this.rollOverPolicy = (String)mp.get("rolloverpolicy");
        }
        if (mp.get("threadcount") != null) {
            this.threadCount = this.getInt(mp, "threadcount");
        }
        if (mp.get("keystoretype") != null) {
            this.keyStoreType = (String)mp.get("keystoretype");
        }
        if (mp.get("keystore") != null) {
            this.keyStore = (String)mp.get("keystore");
        }
        if (mp.get("keystorepassword") != null) {
            this.keyStorePassword = ((Password)mp.get("keystorepassword")).getPlain();
        }
        this.authenticateClient = this.getBoolean("authenticateclient", false);
        if (mp.get("alias") != null) {
            this.aliasConfigFile = (String)mp.get("alias");
        }
        this.maxConcurrentClients = this.getInt("maxConcurrentClient", 5);
        this.authenticationFileLocation = this.getString("authfilelocation", "");
        this.dontBlockOnEOF = this.getBoolean("dontBlockOnEOF", false);
        this.connectionFactoryName = this.getString("connectionfactoryname", null);
        this.messageType = this.getString(Property.MESSAGE_TYPE, "TextMessage");
        this.threadPoolSize = this.getInt(Property.THREAD_POOL_SIZE, 20);
        this.groupPattern = this.getString(Property.GROUP_PATTERN, null);
        this.authenticationPolicy = this.getString("authenticationpolicy", null);
        this.hadoopConfigurationPath = this.getString("hadoopconfigurationpath", null);
    }
    
    public String getString(final String key, final String defaultValue) {
        Object obj = null;
        if (this.propMap != null && (obj = this.propMap.get(key)) != null) {
            return (String)obj;
        }
        return defaultValue;
    }
    
    public String getPassword(final String key) {
        String sPassword = null;
        final Object password = this.propMap.get(key);
        if (password != null) {
            if (password instanceof Password) {
                sPassword = ((Password)password).getPlain();
            }
            else {
                sPassword = (String)password;
            }
        }
        return sPassword;
    }
    
    public boolean getBoolean(final String key, final boolean defaultValue) {
        boolean bool = defaultValue;
        final Object obj;
        if (this.propMap != null && (obj = this.propMap.get(key)) != null) {
            if (obj instanceof Boolean) {
                bool = (boolean)obj;
            }
            else if (obj instanceof String) {
                if (((String)obj).equalsIgnoreCase("true") || ((String)obj).equalsIgnoreCase("yes")) {
                    bool = true;
                }
                else if (((String)obj).equalsIgnoreCase("false") || ((String)obj).equalsIgnoreCase("no")) {
                    bool = false;
                }
            }
        }
        return bool;
    }
    
    public char getChar(final String key, final char defaultValue) {
        char value = defaultValue;
        final Object obj;
        if (this.propMap != null && (obj = this.propMap.get(key)) != null) {
            value = (char)obj;
        }
        return value;
    }
    
    public Object getObject(final String key, final Object defaultValue) {
        Object obj = defaultValue;
        if (this.propMap != null && this.propMap.get(key) != null) {
            obj = this.propMap.get(key);
        }
        return obj;
    }
    
    public int getInt(final String key, final int defaultValue) {
        int value = defaultValue;
        final Object obj;
        if (this.propMap != null && (obj = this.propMap.get(key)) != null) {
            try {
                value = (int)obj;
            }
            catch (ClassCastException ce) {
                try {
                    value = Integer.parseInt(obj.toString());
                }
                catch (Exception e) {
                    value = defaultValue;
                }
            }
        }
        return value;
    }
    
    public Map<String, Object> getMap() {
        return this.propMap;
    }
    
    static {
        Property.COMPRESSION_TYPE = "compressiontype";
        Property.ARCHIVE_TYPE = "archiveType";
        Property.CHARSET = "charset";
        Property.READER_TYPE = "readerType";
        Property.IPADDRESS = "ipaddress";
        Property.DIRECTORY = "directory";
        Property.WILDCARD = "wildcard";
        Property.GROUP_PATTERN = "grouppattern";
        Property.THREAD_POOL_SIZE = "threadpoolsize";
        Property.YIELD_AFTER = "yieldafter";
        Property.NETWORK_FILE_SYSTEM = "networkfilesystem";
        Property.POLL_INTERVAL = "pollinterval";
        Property.DEFAULT_YIELD_AFTER = 20;
        Property.SOURCE_UUID = "sourceUUID";
        Property.MESSAGE_TYPE = "messagetype";
        Property.ROLLOVER_STYLE = "rolloverstyle";
        Property.DEFAULT_FILE_SEQUENCER = "com.datasphere.source.lib.directory.DefaultFileSequencer";
    }
}
