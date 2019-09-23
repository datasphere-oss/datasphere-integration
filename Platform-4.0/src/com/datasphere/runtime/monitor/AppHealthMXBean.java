package com.datasphere.runtime.monitor;

public interface AppHealthMXBean
{
    String getFqAppName();
    
    String getStatus();
    
    long getLastModifiedTime();
}
