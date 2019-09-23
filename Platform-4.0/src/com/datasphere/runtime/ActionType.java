package com.datasphere.runtime;

public enum ActionType
{
    CREATE, 
    START, 
    VERIFY_START, 
    START_SOURCES, 
    STOP, 
    QUIESCE, 
    PAUSE_SOURCES, 
    APPROVE_QUIESCE, 
    UNPAUSE_SOURCES, 
    CHECKPOINT, 
    QUIESCE_FLUSH, 
    QUIESCE_CHECKPOINT, 
    CRASH, 
    DEPLOY, 
    INIT_CACHES, 
    START_CACHES, 
    STOP_CACHES, 
    UNDEPLOY, 
    RESUME, 
    STATUS, 
    MEMORY_STATUS, 
    START_ADHOC, 
    STOP_ADHOC, 
    CONTINUE, 
    IGNORE, 
    RETRY, 
    SHUTDOWN, 
    LOAD, 
    UNLOAD, 
    BOOT, 
    RESTART;
    
    public static boolean contains(final String val) {
        for (final ActionType et : values()) {
            if (et.name().equalsIgnoreCase(val)) {
                return true;
            }
        }
        return false;
    }
}
