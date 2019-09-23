package com.datasphere.historicalcache;

public class Constants
{
    public static String WRONG_SNAPSHOT_ID;
    public static String LOCAL_SNAPSHOT_ID;
    public static String CALLBACK_SID_RECEIVED;
    public static String OLD_SNAPSHOT_ID;
    public static String NEW_SNAPSHOT_ID;
    public static String STATUS_IS_NULL;
    public static final String CACHE_REPLICAS = "REPLICA";
    public static final String CACHE_INSERT_SIZE = "INSERT_RATE";
    public static final String CACHE_INSERT_INFO = "INSERT_INFO";
    public static final String RERESH_INTERVAL = "refreshinterval";
    public static final String CACHE_SNAPSHOT_NAME_SEPARATOR = "-";
    public static final String MSG_EXC_DATA_TYPE_CREATE_FAIL = "Unable to create data type '%s' in EventTable '%s'";
    public static final String MSG_WRN_JAVA_TYPE_UNSUPPORTED = "Unsupported Java data type, '%s' for attribute '%s' in type '%s'";
    public static final String WHERE_QUERY = " { \"select\": [ \"*\" ],  \"from\":   [ \"%s\" ]  }";
    public static final String POSITION_BY_EOF = "positionbyeof";
    public static final String BREAK_ON_NO_RECORD = "breakonnorecord";
    public static final String ENABLE_FLM = "enableFLM";
    
    static {
        Constants.WRONG_SNAPSHOT_ID = "Cache %s local snapshot id cannot be less than zero";
        Constants.LOCAL_SNAPSHOT_ID = "Cache %s local snapshot id is: %s";
        Constants.CALLBACK_SID_RECEIVED = "Cache %s received SID from refresh: %s";
        Constants.OLD_SNAPSHOT_ID = "Cache %s previously had snapshotId is: %s";
        Constants.NEW_SNAPSHOT_ID = "Cache %s now has snapshotId is: %s";
        Constants.STATUS_IS_NULL = "Cache %s status is null";
    }
}
